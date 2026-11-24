from concurrent import futures
import grpc
from googol import googol_pb2
from googol import googol_pb2_grpc
from google.protobuf import empty_pb2
from collections import Counter
from googol.url_queue import URLQueue
import configparser
import threading
import time
import json
import os


class GatewayServicer(googol_pb2_grpc.GatewayServicer):
    def __init__(self, state_file, timeout, lease):
        self.state_file = state_file    #ficheiro onde o estado e guardado
        self.timeout = timeout          #tempo maximo de espera por um barrel

        self.queue = URLQueue(lease)    #fila de url's (ver googol/url_queue.py)
        self.barrels = {}               #nome - {"address", "stub", "index_size", "total_time", "searches"}
        self.searchCounts = Counter()   #pesquisa -nº de vezes que foi feita
        self.nextBarrel = 0             #round-robin - proximo barrel a usar
        self.statsVersion = 0           

        self.lock = threading.RLock()
        self.statsCondition = threading.Condition(self.lock)

        self.loadState()

    #fila de url's (agora com lease: o url volta a fila se o downloader nao chamar done())
    def putNew(self, request, context):
        #coloca url na fila se ainda nao foi visto. E um URL pedido pelo utilizador: passa a frente dos links encontrados pelos downloaders
        if self.queue.push([request.url], priority=True) > 0:
            self.saveState()
            print(f"putNew(): {request.url}")
        return empty_pb2.Empty()

    def putMany(self, request, context):
        #versao em lote (usada pelos Downloaders): uma so gravacao de estado por pagina
        added = self.queue.push(request.urls)
        if added > 0:
            self.saveState()
        return googol_pb2.PushReply(accepted=added)

    def takeNext(self, request, context):
        #entrega o prox. url a UM so downloader. devolve "" se a fila estiver vazia
        url = self.queue.pop()
        if url is None:
            return googol_pb2.TakeNextResponse(url="")
        self.saveState()
        print(f"takeNext(): {url}")
        return googol_pb2.TakeNextResponse(url=url)

    def done(self, request, context):
        #downloader confirma que terminou o url
        self.queue.done(request.url)
        self.saveState()
        return empty_pb2.Empty()

    # Pesquisas (Client)
    def searchWords(self, request, context):
        #pesquina num barrel e devolve 10 resultados. se falhar tenta o seguinte (failover)
        words = [word.lower() for word in request.words]

        if request.page == 0:   
            with self.lock:
                self.searchCounts[" ".join(words)] += 1
                self.saveState()

        for name, stub in self.barrelOrder():
            try:
                start = time.time()
                response = stub.searchWords(googol_pb2.SearchWordsRequest(words=words), timeout=self.timeout)
                self.recordSearchTime(name, time.time() - start)

                first = request.page * 10
                return googol_pb2.SearchWordsResponse(pages=response.pages[first:first + 10])
            except grpc.RpcError as e:
                print(f"Barrel {name} falhou na pesquisa: {e.code()}")
                self.removeBarrel(name)

        context.abort(grpc.StatusCode.UNAVAILABLE, "Nenhum Barrel disponivel")

    def getBacklinks(self, request, context):
        #pede a barrel as paginas com ligacao para url
        for name, stub in self.barrelOrder():
            try:
                return stub.getBacklinks(request, timeout=self.timeout)
            except grpc.RpcError as e:
                print(f"Barrel {name} falhou: {e.code()}")
                self.removeBarrel(name)

        context.abort(grpc.StatusCode.UNAVAILABLE, "Nenhum Barrel disponivel")

    #estatisticas  
    def getStats(self, request, context):
        #envia as estatisticas ao cliente sempre que mudam
        lastVersion = -1
        while context.is_active():
            with self.statsCondition:
                if self.statsVersion == lastVersion:
                    #timeout para verificar se o cliente ainda esta ativo
                    self.statsCondition.wait(timeout=1)
                    continue
                lastVersion = self.statsVersion
                stats = self.buildStats()
            yield stats

    def buildStats(self):
        #mensagem com estatisticas atuais
        with self.lock:
            stats = googol_pb2.StatsResponse()
            for search, count in self.searchCounts.most_common(10):
                stats.top_searches.append(f"{search} ({count})")
            for name, barrel in self.barrels.items():
                avg = 0.0
                if barrel["searches"] > 0:
                    avg = barrel["total_time"] / barrel["searches"] * 10   #decimas de segundo
                stats.barrels.append(googol_pb2.BarrelStats(
                    name=name, index_size=barrel["index_size"], avg_response_time=avg))
            return stats

    def statsChanged(self):
        #avisa os streams de getStats da mudanca das estatisticas
        with self.statsCondition:
            self.statsVersion += 1
            self.statsCondition.notify_all()

    #barrels
    def registerBarrel(self, request, context):
        #barrel regista-se ao arrancar
        with self.lock:
            known = self.barrels.get(request.name)
            if known is not None and known["address"] == request.address:
                return empty_pb2.Empty()   #ja registado (o barrel repete o registo): nao repor as estatisticas
            self.addBarrel(request.name, request.address)
            self.saveState()
            self.statsChanged()
        print(f"registerBarrel(): {request.name} em {request.address}")
        return empty_pb2.Empty()

    def updateIndexSize(self, request, context):
        #mudança de indice
        with self.lock:
            barrel = self.barrels.get(request.name)
            if barrel is not None and barrel["index_size"] != request.index_size:
                barrel["index_size"] = request.index_size   #so avisa os clientes se mudou
                self.statsChanged()
        return empty_pb2.Empty()

    def addBarrel(self, name, address):
        channel = grpc.insecure_channel(address)
        self.barrels[name] = {
            "address": address,
            "stub": googol_pb2_grpc.BarrelStub(channel),
            "index_size": 0,
            "total_time": 0.0,
            "searches": 0,
        }

    def removeBarrel(self, name):
        with self.lock:
            if name in self.barrels:
                del self.barrels[name]
                self.saveState()
                self.statsChanged()
                print(f"Barrel {name} removido")

    def barrelOrder(self):
        #devolve barrels ativos
        with self.lock:
            names = list(self.barrels.keys())
            if len(names) == 0:
                return []
            self.nextBarrel = (self.nextBarrel + 1) % len(names)
            names = names[self.nextBarrel:] + names[:self.nextBarrel]
            return [(name, self.barrels[name]["stub"]) for name in names]

    def recordSearchTime(self, name, seconds):
        with self.lock:
            if name in self.barrels:
                self.barrels[name]["total_time"] += seconds
                self.barrels[name]["searches"] += 1
            self.statsChanged()

    #persistencia 
    def saveState(self):
        with self.lock:
            state = {
                "queue": self.queue.export_state(),
                "barrels": {name: barrel["address"] for name, barrel in self.barrels.items()},
                "searchCounts": dict(self.searchCounts),
            }
            with open(self.state_file + ".tmp", "w") as f:
                json.dump(state, f)
            os.replace(self.state_file + ".tmp", self.state_file)   #evita que o ficheiro fique a meio

    def loadState(self):
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file) as f:
                state = json.load(f)
            self.queue.load_state(state["queue"])
            self.searchCounts = Counter(state["searchCounts"])
            for name, address in state["barrels"].items():
                self.addBarrel(name, address)
            print(f"Estado recuperado: {self.queue.stats()['pending']} URLs na fila, {len(self.barrels)} Barrels")
        except Exception as e:
            print(f"Erro a ler o estado ({e}), a comecar do zero")


def serve():
    config = configparser.ConfigParser()
    config.read("config.properties")
    server_port = config.getint("googol", "gateway_port")
    state_file = config.get("googol", "gateway_state_file")
    timeout = config.getfloat("googol", "barrel_timeout")
    lease = config.getfloat("googol", "url_lease", fallback=60)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    googol_pb2_grpc.add_GatewayServicer_to_server(GatewayServicer(state_file, timeout, lease), server)
    server.add_insecure_port("0.0.0.0:{}".format(server_port))
    server.start()
    print("Gateway started on port {}".format(server_port))

    server.wait_for_termination()


if __name__ == "__main__":
    serve()

import grpc
from googol import googol_pb2
from googol import googol_pb2_grpc
from google.protobuf import empty_pb2
import configparser
import threading
import time


class StatsListener(threading.Thread):
    #recebe as estatisticas da gatewat e print sempre que mudam. 
    def __init__(self, stub, retry_delay):
        super().__init__(name="StatsListener", daemon=True)
        self.stub = stub
        self.retry_delay = retry_delay
        self.stopped = False
        self.stream = None
        self.start()

    def run(self):
        while not self.stopped:
            try:
                self.stream = self.stub.getStats(empty_pb2.Empty())
                for stats in self.stream:
                    if self.stopped:
                        break
                    printStats(stats)
            except grpc.RpcError:
                if not self.stopped:
                    print("Ligacao a Gateway perdida, a tentar de novo")
                    time.sleep(self.retry_delay)

    def stop(self):
        self.stopped = True
        if self.stream is not None:
            self.stream.cancel()


def printStats(stats):
    print("10 pesquisas mais comuns:")
    for i, search in enumerate(stats.top_searches):
        print(f"  {i + 1}. {search}")
    print("Barrels ativos:")
    for barrel in stats.barrels:
        print(f"  {barrel.name}: indice com {barrel.index_size} palavras, "
              f"tempo medio de resposta {barrel.avg_response_time:.2f} decimas de segundo")


def call(function, retries, retry_delay):
    #executa chamada à gateway. se falhar dá retry. 
    for attempt in range(retries):
        try:
            return function()
        except grpc.RpcError as e:
            print(f"Erro ({e.code()}): {e.details()}.")
            time.sleep(retry_delay)
    return None


def indexUrl(stub, retries, retry_delay):
    url = input("URL a indexar: ").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        print("O URL tem de comecar por http:// ou https://")
        return
    if call(lambda: stub.putNew(googol_pb2.PutNewRequest(url=url)), retries, retry_delay) is not None:
        print("URL adicionado a fila.")


def search(stub, retries, retry_delay):
    words = input("Palavras a pesquisar: ").split()
    if len(words) == 0:
        return

    page = 0
    while True:
        request = googol_pb2.SearchWordsRequest(words=words, page=page)
        response = call(lambda: stub.searchWords(request), retries, retry_delay)
        if response is None:
            return

        if len(response.pages) == 0:
            if page == 0:
                print("Sem resultados.")
                return
            print("Nao ha mais resultados.")
            page -= 1
            continue

        for i, result in enumerate(response.pages):
            print(f"{page * 10 + i + 1}. {result.title}")
            print(f"   {result.url}")
            print(f"   {result.citation}")
            print(f"   ({result.backlinks} ligacoes recebidas)")

        option = input("\n[s] seguinte  [a] anterior  [Enter] voltar ao menu: ").strip().lower()
        if option == "s" and len(response.pages) == 10:
            page += 1
        elif option == "s":
            print("Nao ha mais resultados.")
        elif option == "a" and page > 0:
            page -= 1
        elif option == "a":
            print("Ja esta na primeira pagina.")
        else:
            return


def backlinks(stub, retries, retry_delay):
    url = input("URL: ").strip()
    request = googol_pb2.GetBacklinksRequest(url=url)
    response = call(lambda: stub.getBacklinks(request), retries, retry_delay)
    if response is None:
        return
    if len(response.urls) == 0:
        print("Nenhuma pagina conhecida tem ligacao para este URL.")
    for link in response.urls:
        print(f"  {link}")


def stats(stub, retry_delay):
    listener = StatsListener(stub, retry_delay)
    input()   #fica a mostrar as estatisticas ate o utilizador carregar Enter
    listener.stop()


def run():
    config = configparser.ConfigParser()
    config.read("config.properties")
    gateway_address = "{}:{}".format(config.get("googol", "gateway_host"), config.getint("googol", "gateway_port"))
    retries = config.getint("googol", "retries")
    retry_delay = config.getfloat("googol", "retry_delay")

    #cria canal grpc
    channel = grpc.insecure_channel(gateway_address)

    #cria stub 
    stub = googol_pb2_grpc.GatewayStub(channel)

    try:
        while True:
            print("1. Indexar novo URL")
            print("2. Pesquisar")
            print("3. Paginas com ligacao para um URL")
            print("4. Estatisticas")
            print("0. Sair")
            option = input("Opcao: ").strip()

            if option == "1":
                indexUrl(stub, retries, retry_delay)
            elif option == "2":
                search(stub, retries, retry_delay)
            elif option == "3":
                backlinks(stub, retries, retry_delay)
            elif option == "4":
                stats(stub, retry_delay)
            elif option == "0":
                break
            else:
                print("Opcao invalida.")

    except KeyboardInterrupt:
        print("\nStopping the client...")


if __name__ == '__main__':
    run()

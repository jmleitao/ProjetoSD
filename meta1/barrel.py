import argparse
import threading
import time
from concurrent import futures

import grpc
from google.protobuf import empty_pb2

from googol import googol_pb2 as pb
from googol import googol_pb2_grpc as pb_grpc
from googol.barrel_store import BarrelStore
from googol.config import config_defaults


TIMEOUT = 5  # segundos por chamada à Gateway
SNAPSHOT_TIMEOUT = 600  # o snapshot pode ser grande


class BarrelServicer(pb_grpc.BarrelServicer):
    def __init__(self, name: str, store: BarrelStore):
        self.name = name
        self.store = store

    def addToIndex(self, request, context):
        # Se apply() levantar exceção, o gRPC devolve erro => sem ACK => o Downloader repete.
        if self.store.apply(request):
            print(f"[{self.name}] indexado {request.url} ({request.msg_id})")
        return empty_pb2.Empty()  # regressar sem erro = ACK (também para duplicados)

    def searchWords(self, request, context):
        rows = self.store.search(list(request.words))
        return pb.SearchWordsResponse(pages=[
            pb.PageInfo(url=u, title=t or "", citation=c or "", backlinks=bl)
            for u, t, c, bl in rows])  # todos os resultados: a Gateway é que pagina

    def getBacklinks(self, request, context):
        return pb.GetBacklinksResponse(urls=self.store.backlinks(request.url))

    def getSnapshot(self, request, context):
        for msg in self.store.snapshot():
            yield pb.AddToIndexRequest(**msg)


def sync_from_peer(store: BarrelStore, peer_address: str) -> int:
    """Recupera de uma avaria: pede ao outro Barrel o que já aplicou e reaplica (idempotente)."""
    channel = grpc.insecure_channel(peer_address)
    peer, applied = pb_grpc.BarrelStub(channel), 0
    try:
        for msg in peer.getSnapshot(empty_pb2.Empty(), timeout=SNAPSHOT_TIMEOUT):
            if store.apply(msg):
                applied += 1
    except grpc.RpcError as e:
        print(f"sync interrompido ({e.code()}); o resto chega pelo multicast")
    finally:
        channel.close()
    return applied


def report_to_gateway(store, name, address, gateway_address):
    """Registo repetido de 5 em 5 s (a Gateway ignora repetições) + tamanho do índice."""
    gateway = pb_grpc.GatewayStub(grpc.insecure_channel(gateway_address))  # reconecta-se sozinho
    last, tick = None, 0
    while True:
        try:
            if tick % 5 == 0:
                gateway.registerBarrel(pb.RegisterBarrelRequest(name=name, address=address),
                                       timeout=TIMEOUT)
                last = None  # depois de (re)registado, volta a enviar o tamanho
            if store.vocab_size != last:
                gateway.updateIndexSize(pb.UpdateIndexSizeRequest(name=name, index_size=store.vocab_size),
                                        timeout=TIMEOUT)
                last = store.vocab_size
        except grpc.RpcError:
            last, tick = None, -1  # Gateway em baixo: regista-se de novo assim que voltar
        tick += 1
        time.sleep(1)


if __name__ == "__main__":
    default_gw, _ = config_defaults()
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--name", required=True, help="nome único na Gateway (ex.: barrel1)")
    ap.add_argument("--host", default="127.0.0.1", help="endereço pelo qual a Gateway chega a este Barrel")
    ap.add_argument("--db", help="ficheiro SQLite (por omissão <name>.db)")
    ap.add_argument("--gateway", default=default_gw)
    ap.add_argument("--peer", help="host:porta do outro Barrel, para sincronizar no arranque")
    args = ap.parse_args()

    store = BarrelStore(args.db or f"{args.name}.db")
    print(f"[{args.name}] base de dados com {store.vocab_size} palavras")
    if args.peer:
        n = sync_from_peer(store, args.peer)
        print(f"[{args.name}] sincronizado a partir de {args.peer}: {n} mensagens novas")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_BarrelServicer_to_server(BarrelServicer(args.name, store), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"[{args.name}] pronto na porta {args.port}")
    threading.Thread(target=report_to_gateway, daemon=True,
                     args=(store, args.name, f"{args.host}:{args.port}", args.gateway)).start()
    server.wait_for_termination()

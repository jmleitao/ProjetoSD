import argparse
import logging
import time

import grpc
from google.protobuf import empty_pb2

from googol import googol_pb2 as pb
from googol import googol_pb2_grpc as pb_grpc
from googol.config import config_defaults
from googol.multicast import ReliableMulticaster
from googol.parser import fetch_and_parse, normalize

log = logging.getLogger("downloader")

TIMEOUT = 5  # segundos por chamada à Gateway


def run(args):
    gateway = pb_grpc.GatewayStub(grpc.insecure_channel(args.gateway))  # o canal reconecta-se sozinho
    mc = ReliableMulticaster(args.id, args.barrels, args.outbox_dir)

    for seed in args.seed:
        try:
            gateway.putMany(pb.UrlList(urls=[normalize(seed)]), timeout=TIMEOUT)
        except grpc.RpcError as e:
            log.error("não consegui inserir seed %s: %s", seed, e.code())

    while True:
        try:
            url = gateway.takeNext(empty_pb2.Empty(), timeout=TIMEOUT).url  # "" = fila vazia
            if not url:
                time.sleep(1)
                continue
            log.info("a visitar %s", url)

            try:
                page = fetch_and_parse(url)
            except Exception as e:  # HTTP, timeout, não-HTML...
                log.warning("falhou %s: %s", url, e)
                gateway.done(pb.UrlRequest(url=url), timeout=TIMEOUT)
                continue

            # 1º multicast (fica gravado em disco), 2º novos links, 3º confirma o URL
            mc.multicast({"url": url, "title": page["title"], "citation": page["citation"],
                          "words": page["words"], "links": page["links"]})
            gateway.putMany(pb.UrlList(urls=page["links"]), timeout=TIMEOUT)
            gateway.done(pb.UrlRequest(url=url), timeout=TIMEOUT)
            time.sleep(args.delay)  # cortesia para com os servidores
        except grpc.RpcError as e:
            # Gateway indisponível: voltar a tentar sem perder o Downloader
            log.warning("Gateway indisponível (%s); a repetir", e.code())
            time.sleep(2)


if __name__ == "__main__":
    default_gw, default_barrels = config_defaults()
    p = argparse.ArgumentParser()
    p.add_argument("--id", required=True, help="identificador estável deste Downloader (ex.: d1)")
    p.add_argument("--seed", action="append", default=[], help="URL inicial (repetível)")
    p.add_argument("--gateway", default=default_gw)
    p.add_argument("--barrels", nargs="+", default=default_barrels, help="host:porta de cada Barrel")
    p.add_argument("--outbox-dir", default=".")
    p.add_argument("--delay", type=float, default=0.5)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run(p.parse_args())

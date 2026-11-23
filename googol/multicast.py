import json
import logging
import os
import queue
import threading

log = logging.getLogger("multicast")


TIMEOUT = 5  # segundos por chamada ao Barrel


class _GrpcBarrel:
    """Ligação de uma thread de envio a um Barrel. Regressar de addToIndex sem erro = ACK."""

    def __init__(self, address: str):
        # imports tardios: os testes (com Barrels falsos) não precisam de gRPC nem do código gerado
        import grpc
        from googol import googol_pb2, googol_pb2_grpc
        self._grpc, self._pb = grpc, googol_pb2
        self._stub_class = googol_pb2_grpc.BarrelStub
        self.address = address
        self._open()

    def _open(self):
        self._channel = self._grpc.insecure_channel(self.address)
        self._stub = self._stub_class(self._channel)

    def index_page(self, msg: dict) -> bool:
        """msg = dict com msg_id, url, title, citation, words, links."""
        self._stub.addToIndex(self._pb.AddToIndexRequest(**msg), timeout=TIMEOUT)
        return True  # se houve erro, a exceção (grpc.RpcError) sobe e o multicast repete

    def reset(self):
        """Recria o canal depois de uma falha."""
        self._channel.close()
        self._open()


def _grpc_client(address: str):
    return _GrpcBarrel(address)


class Outbox:
    """Log em disco (JSON lines): registos 'm' (mensagem) e 'a' (ACK de um Barrel)."""

    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._msgs = {}   # msg_id -> mensagem
        self._acked = {}  # barrel -> {msg_id}
        self.last_seq = 0
        if os.path.exists(path):
            self._load(path)
        self._file = open(path, "a", encoding="utf-8")

    def _load(self, path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue  # última linha pode ter ficado cortada numa avaria
                if rec["t"] == "m":
                    self._msgs[rec["msg"]["msg_id"]] = rec["msg"]
                    self.last_seq = max(self.last_seq, int(rec["msg"]["msg_id"].rsplit(":", 1)[1]))
                elif rec["t"] == "a":
                    self._acked.setdefault(rec["b"], set()).add(rec["id"])

    def _write(self, rec):
        with self._lock:
            self._file.write(json.dumps(rec) + "\n")
            self._file.flush()
            os.fsync(self._file.fileno())

    def log_message(self, msg):
        self._msgs[msg["msg_id"]] = msg
        self._write({"t": "m", "msg": msg})

    def log_ack(self, barrel, msg_id):
        self._acked.setdefault(barrel, set()).add(msg_id)
        self._write({"t": "a", "b": barrel, "id": msg_id})

    def pending(self, barrel):
        done = self._acked.get(barrel, set())
        todo = [m for i, m in self._msgs.items() if i not in done]
        return sorted(todo, key=lambda m: int(m["msg_id"].rsplit(":", 1)[1]))

    def close(self):
        self._file.close()


class ReliableMulticaster:
    def __init__(self, downloader_id, barrel_names, outbox_dir=".",
                 proxy_factory=_grpc_client, max_backoff=10.0):
        self.downloader_id = downloader_id
        self._factory = proxy_factory
        self._max_backoff = max_backoff
        self._outbox = Outbox(os.path.join(outbox_dir, f"outbox_{downloader_id}.jsonl"))
        self._seq = self._outbox.last_seq  # retoma a numeração após reinício
        self._seq_lock = threading.Lock()
        self._stop = threading.Event()
        self._queues = {b: queue.Queue() for b in barrel_names}
        self._threads = []
        for b in barrel_names:
            for m in self._outbox.pending(b):  # retoma o que ficou por entregar
                self._queues[b].put(m)
            t = threading.Thread(target=self._sender, args=(b,), daemon=True, name=f"mc-{b}")
            t.start()
            self._threads.append(t)

    def multicast(self, payload: dict) -> str:
        """Regista a mensagem (durável) e agenda o envio a todos os Barrels. Não bloqueia."""
        with self._seq_lock:
            self._seq += 1
            msg_id = f"{self.downloader_id}:{self._seq}"
        msg = dict(payload, msg_id=msg_id)
        self._outbox.log_message(msg)
        for q in self._queues.values():
            q.put(msg)
        return msg_id

    def pending_counts(self) -> dict:
        return {b: q.qsize() for b, q in self._queues.items()}

    def _sender(self, barrel):
        # Cada thread tem o seu cliente (e canal) para não haver contenção entre Barrels.
        proxy = self._factory(barrel)
        q = self._queues[barrel]
        while not self._stop.is_set():
            try:
                msg = q.get(timeout=0.2)
            except queue.Empty:
                continue
            delay = 0.5
            while not self._stop.is_set():
                try:
                    if proxy.index_page(msg):
                        self._outbox.log_ack(barrel, msg["msg_id"])
                        break
                    log.warning("%s recusou %s, a repetir", barrel, msg["msg_id"])
                except Exception as e:  # grpc.RpcError (UNAVAILABLE, DEADLINE_EXCEEDED...)
                    log.warning("%s indisponível (%s); retry em %.1fs", barrel, type(e).__name__, delay)
                    try:
                        proxy.reset()  # recria o canal gRPC
                    except Exception:
                        pass
                self._stop.wait(delay)
                delay = min(delay * 2, self._max_backoff)

    def close(self, drain_timeout=0.0):
        """Para as threads. As mensagens por entregar ficam no log para o próximo arranque."""
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)
        self._outbox.close()

import threading
import time
from collections import deque


class URLQueue:
    def __init__(self, lease_seconds: float = 60):
        self._lock = threading.Lock()  # o servidor gRPC atende pedidos em várias threads
        self._pending = deque()
        self._seen = set()
        self._in_progress = {}  # url -> instante limite
        self._lease = lease_seconds

    def push(self, urls, priority: bool = False) -> int:
        """Adiciona URLs novos. Devolve quantos eram novos.

        priority=False (links encontrados pelos Downloaders): vão para o FIM da fila.
        priority=True  (URL sugerido pelo utilizador): vai para a FRENTE da fila, para ser o
        proximo a ser visitado. Se ja estava pendente, passa para a frente."""
        added = 0
        with self._lock:
            for url in urls:
                if url not in self._seen:
                    self._seen.add(url)
                    (self._pending.appendleft if priority else self._pending.append)(url)
                    added += 1
                elif priority and url in self._pending:
                    self._pending.remove(url)
                    self._pending.appendleft(url)
        return added

    def pop(self):
        with self._lock:
            now = time.time()
            for url, deadline in list(self._in_progress.items()):
                if deadline < now:  # lease expirou: devolve à frente da fila
                    del self._in_progress[url]
                    self._pending.appendleft(url)
            if not self._pending:
                return None
            url = self._pending.popleft()
            self._in_progress[url] = now + self._lease
            return url

    def done(self, url: str) -> None:
        with self._lock:
            self._in_progress.pop(url, None)

    def stats(self) -> dict:
        with self._lock:
            return {"pending": len(self._pending),
                    "in_progress": len(self._in_progress),
                    "seen": len(self._seen)}

    def export_state(self) -> dict:
        """Estado serializável. URLs em curso voltam a 'pendentes' (se a Gateway reiniciar,
        os Downloaders que os tinham já não os vão confirmar)."""
        with self._lock:
            return {"pending": list(self._in_progress) + list(self._pending),
                    "seen": list(self._seen)}

    def load_state(self, state: dict) -> None:
        with self._lock:
            self._pending = deque(state["pending"])
            self._seen = set(state["seen"])
            self._in_progress = {}

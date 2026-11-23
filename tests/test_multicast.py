import os, shutil, tempfile, threading, time, unittest

from googol.multicast import ReliableMulticaster
from googol.parser import parse_html
from googol.url_queue import URLQueue


class FakeBarrel:
    """Imita o Barrel: idempotente e pode ser 'desligado'."""
    def __init__(self):
        self.up = True
        self.seen, self.applied, self.calls = set(), 0, 0
        self.lock = threading.Lock()

    def index_page(self, msg):
        with self.lock:
            self.calls += 1
            if not self.up:
                raise ConnectionError("barrel em baixo")
            if msg["msg_id"] not in self.seen:
                self.seen.add(msg["msg_id"]); self.applied += 1
            return True


class FakeProxy:
    def __init__(self, barrel): self.b = barrel
    def index_page(self, msg): return self.b.index_page(msg)
    def reset(self): pass


def wait_for(cond, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        if cond(): return True
        time.sleep(0.05)
    return False


class MulticastTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.b1, self.b2 = FakeBarrel(), FakeBarrel()
        self.factory = lambda name: FakeProxy({"b1": self.b1, "b2": self.b2}[name])
    def tearDown(self): shutil.rmtree(self.dir)
    def make(self):
        return ReliableMulticaster("d1", ["b1", "b2"], self.dir, self.factory, max_backoff=0.2)

    def test_entrega_a_ambos(self):
        mc = self.make()
        for i in range(5): mc.multicast({"url": f"u{i}"})
        self.assertTrue(wait_for(lambda: self.b1.applied == 5 and self.b2.applied == 5))
        mc.close()

    def test_barrel_em_baixo_recupera_sem_perder_nem_duplicar(self):
        self.b2.up = False
        mc = self.make()
        for i in range(5): mc.multicast({"url": f"u{i}"})
        self.assertTrue(wait_for(lambda: self.b1.applied == 5))   # b1 não é atrasado por b2
        self.assertEqual(self.b2.applied, 0)
        self.b2.up = True
        self.assertTrue(wait_for(lambda: self.b2.applied == 5))
        self.assertEqual(self.b2.applied, 5)                       # sem duplicados
        mc.close()

    def test_downloader_reinicia_e_retoma_pendentes(self):
        self.b2.up = False
        mc = self.make()
        for i in range(3): mc.multicast({"url": f"u{i}"})
        wait_for(lambda: self.b1.applied == 3)
        mc.close()                                                 # "avaria" com b2 por entregar
        self.b2.up = True
        mc2 = self.make()                                          # novo arranque, mesmo id
        self.assertTrue(wait_for(lambda: self.b2.applied == 3))
        self.assertEqual(self.b1.applied, 3)                       # b1 não recebe repetidos
        mid = mc2.multicast({"url": "novo"})
        self.assertEqual(mid, "d1:4")                              # numeração continua
        mc2.close()

    def test_duplicado_e_ignorado_pelo_barrel(self):
        msg = {"msg_id": "x:1", "url": "u"}
        self.b1.index_page(msg); self.b1.index_page(msg)
        self.assertEqual(self.b1.applied, 1)


class QueueTests(unittest.TestCase):
    def test_deduplica_e_entrega_a_um_so(self):
        q = URLQueue()
        self.assertEqual(q.push(["a", "b", "a"]), 2)
        self.assertEqual(q.push(["a"]), 0)
        got = {q.pop(), q.pop()}
        self.assertEqual(got, {"a", "b"})
        self.assertIsNone(q.pop())

    def test_lease_expirado_devolve_url(self):
        q = URLQueue(lease_seconds=0.1)
        q.push(["a"])
        self.assertEqual(q.pop(), "a")      # Downloader "morre" sem chamar done()
        time.sleep(0.2)
        self.assertEqual(q.pop(), "a")      # outro Downloader recebe-o
        q.done("a")
        time.sleep(0.2)
        self.assertIsNone(q.pop())          # com done() não volta


class QueueStateTests(unittest.TestCase):
    def test_estado_persiste_e_urls_em_curso_voltam_a_pendentes(self):
        q = URLQueue()
        q.push(["a", "b", "c"])
        q.pop()                                  # "a" fica em curso (Gateway vai reiniciar)
        estado = q.export_state()
        q2 = URLQueue()
        q2.load_state(estado)
        self.assertEqual(q2.stats(), {"pending": 3, "in_progress": 0, "seen": 3})
        self.assertEqual(q2.push(["a"]), 0)      # continua a deduplicar
        self.assertEqual(q2.pop(), "a")          # o URL em curso foi recuperado


class ParserTests(unittest.TestCase):
    def test_parse(self):
        html = """<html><head><title> UC </title><script>var x=1</script></head>
        <body><p>Universidade de Coimbra, Coimbra!</p>
        <a href="/a#frag">a</a><a href="https://x.pt/b">b</a><a href="mailto:z@z.pt">m</a>
        <a href="/a">a2</a></body></html>"""
        r = parse_html(html, "https://www.uc.pt/inicio")
        self.assertEqual(r["title"], "UC")
        self.assertEqual(r["links"], ["https://www.uc.pt/a", "https://x.pt/b"])
        self.assertIn("universidade", r["words"])
        self.assertEqual(r["words"].count("coimbra"), 1)   # palavras únicas
        self.assertIn("uc", r["words"])                    # o título também é indexado
        self.assertNotIn("var", r["words"])                # scripts são ignorados


if __name__ == "__main__":
    unittest.main()

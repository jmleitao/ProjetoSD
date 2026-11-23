import os, shutil, tempfile, unittest
from types import SimpleNamespace as NS

from googol.barrel_store import BarrelStore


def msg(i, url, words, links=(), title=None):
    return NS(msg_id=f"d1:{i}", url=url, title=title or url, citation=f"citação de {url}",
              words=list(words), links=list(links))


class BarrelStoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "b.db")
        self.s = BarrelStore(self.path)

    def tearDown(self):
        self.s.close(); shutil.rmtree(self.dir)

    def test_duplicado_nao_altera_nada(self):
        m = msg(1, "http://a", ["universidade"], ["http://b"])
        self.assertTrue(self.s.apply(m))
        self.assertFalse(self.s.apply(m))
        self.assertEqual(self.s.vocab_size, 1)
        self.assertEqual(self.s.backlinks("http://b"), ["http://a"])

    def test_dados_persistem_depois_do_apply(self):
        self.s.apply(msg(1, "http://a", ["coimbra"]))
        outro = BarrelStore(self.path)           # abrir sem fechar o primeiro = "depois de um crash"
        self.assertEqual([r[0] for r in outro.search(["coimbra"])], ["http://a"])
        self.assertEqual(outro.vocab_size, 1)
        self.assertFalse(outro.apply(msg(1, "http://a", ["coimbra"])))  # e continua a deduplicar
        outro.close()

    def test_pesquisa_exige_todos_os_termos_e_ordena_por_backlinks(self):
        self.s.apply(msg(1, "http://a", ["universidade", "coimbra"], ["http://b"]))
        self.s.apply(msg(2, "http://b", ["universidade", "lisboa"]))
        self.s.apply(msg(3, "http://c", ["universidade", "porto"], ["http://b"]))
        res = self.s.search(["Universidade"])    # maiúsculas não importam
        self.assertEqual(res[0][0], "http://b")  # 2 ligações recebidas
        self.assertEqual(res[0][3], 2)
        self.assertEqual({r[0] for r in res}, {"http://a", "http://b", "http://c"})
        self.assertEqual([r[0] for r in self.s.search(["universidade", "coimbra"])], ["http://a"])
        self.assertEqual(self.s.search(["inexistente"]), [])
        self.assertEqual(self.s.search([]), [])

    def test_backlinks_contam_antes_de_o_destino_ser_indexado(self):
        self.s.apply(msg(1, "http://a", ["x"], ["http://futuro"]))
        self.assertEqual(self.s.backlinks("http://futuro"), ["http://a"])

    def test_pagina_nao_e_backlink_de_si_propria(self):
        self.s.apply(msg(1, "http://a", ["x"], ["http://a"]))
        self.assertEqual(self.s.backlinks("http://a"), [])

    def test_snapshot_sincroniza_outra_replica(self):
        for i in range(1, 6):
            self.s.apply(msg(i, f"http://p{i}", ["comum", f"w{i}"], ["http://p1"]))
        d2 = tempfile.mkdtemp()
        try:
            r2 = BarrelStore(os.path.join(d2, "r2.db"))
            r2.apply(msg(1, "http://p1", ["comum", "w1"], ["http://p1"]))   # já tinha uma
            for m in self.s.snapshot(chunk=2):                              # blocos pequenos
                r2.apply(NS(**m))
            self.assertEqual(r2.vocab_size, self.s.vocab_size)
            self.assertEqual(r2.search(["comum"]), self.s.search(["comum"]))
            self.assertEqual(r2.backlinks("http://p1"), self.s.backlinks("http://p1"))
            r2.close()
        finally:
            shutil.rmtree(d2)

    def test_erro_a_meio_nao_deixa_dados_a_meio(self):
        mau = msg(1, "http://a", ["x"])
        mau.links = [object()]                   # força erro dentro da transação
        with self.assertRaises(Exception):
            self.s.apply(mau)
        self.assertEqual(self.s.search(["x"]), [])
        self.assertEqual(self.s.vocab_size, 0)
        self.assertTrue(self.s.apply(msg(1, "http://a", ["x"])))   # o msg_id não ficou "queimado"


if __name__ == "__main__":
    unittest.main()

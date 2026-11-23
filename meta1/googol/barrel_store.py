import json
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (msg_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pages (url TEXT PRIMARY KEY, title TEXT, citation TEXT);
CREATE TABLE IF NOT EXISTS words (word TEXT NOT NULL, url TEXT NOT NULL,
                                  PRIMARY KEY (word, url)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS vocab (word TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS links (src TEXT NOT NULL, dst TEXT NOT NULL,
                                  PRIMARY KEY (src, dst)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS links_dst ON links (dst);
"""


class BarrelStore:
    def __init__(self, path: str):
        self._lock = threading.Lock()  # o servidor gRPC atende pedidos em várias threads
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(SCHEMA)
        self._vocab = self._db.execute("SELECT COUNT(*) FROM vocab").fetchone()[0]

    def apply(self, m) -> bool:
        """m tem: msg_id, url, title, citation, words, links. True = aplicada; False = duplicado."""
        words = sorted({w.lower() for w in m.words if w})
        links = sorted({x for x in m.links if x and x != m.url})  # uma página não conta como backlink de si
        payload = json.dumps({"msg_id": m.msg_id, "url": m.url, "title": m.title,
                              "citation": m.citation, "words": list(m.words),
                              "links": list(m.links)}, ensure_ascii=False)
        with self._lock:
            if m.msg_id and self._db.execute(
                    "SELECT 1 FROM messages WHERE msg_id = ?", (m.msg_id,)).fetchone():
                return False
            with self._db:  # transação (COMMIT no fim, ROLLBACK se der erro)
                if m.msg_id:
                    self._db.execute("INSERT INTO messages VALUES (?, ?)", (m.msg_id, payload))
                self._db.execute("INSERT OR REPLACE INTO pages VALUES (?, ?, ?)",
                                 (m.url, m.title, m.citation))
                self._db.executemany("INSERT OR IGNORE INTO words VALUES (?, ?)",
                                     [(w, m.url) for w in words])
                added = self._db.executemany("INSERT OR IGNORE INTO vocab VALUES (?)",
                                             [(w,) for w in words]).rowcount
                self._db.executemany("INSERT OR IGNORE INTO links VALUES (?, ?)",
                                     [(m.url, x) for x in links])
            self._vocab += added
        return True

    def search(self, terms):
        """Páginas com TODOS os termos, por nº de backlinks (desc). [(url, título, citação, backlinks)]"""
        terms = sorted({t.lower() for t in terms if t})
        if not terms:
            return []
        marks = ",".join("?" * len(terms))
        sql = f"""
            SELECT p.url, p.title, p.citation,
                   (SELECT COUNT(*) FROM links l WHERE l.dst = p.url) AS bl
            FROM pages p
            WHERE p.url IN (SELECT url FROM words WHERE word IN ({marks})
                            GROUP BY url HAVING COUNT(DISTINCT word) = ?)
            ORDER BY bl DESC, p.url"""
        with self._lock:
            return self._db.execute(sql, (*terms, len(terms))).fetchall()

    def backlinks(self, url: str):
        with self._lock:
            return [r[0] for r in self._db.execute(
                "SELECT src FROM links WHERE dst = ? ORDER BY src", (url,))]

    def snapshot(self, chunk: int = 200):
        """Gera (dicts) todas as mensagens já aplicadas, por blocos, sem bloquear o Barrel."""
        last = ""
        while True:
            with self._lock:
                rows = self._db.execute(
                    "SELECT msg_id, payload FROM messages WHERE msg_id > ? ORDER BY msg_id LIMIT ?",
                    (last, chunk)).fetchall()
            if not rows:
                return
            for _, payload in rows:
                yield json.loads(payload)
            last = rows[-1][0]

    @property
    def vocab_size(self) -> int:
        return self._vocab

    def close(self):
        self._db.close()

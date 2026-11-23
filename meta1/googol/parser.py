import re
from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "GoogolBot/1.0 (projeto SD - Universidade de Coimbra)"}
WORD_RE = re.compile(r"\w+", re.UNICODE)
QUOTE_LEN = 200


def normalize(url: str) -> str:
    """Remove o #fragmento e espaços, para o mesmo URL não entrar duas vezes na fila."""
    return urldefrag(url.strip())[0]


def parse_html(html: str, base_url: str) -> dict:
    """Extrai título, citação, palavras (únicas, minúsculas) e links absolutos."""
    soup = BeautifulSoup(html, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        link = normalize(urljoin(base_url, a["href"]))
        if urlparse(link).scheme in ("http", "https"):
            links.append(link)
    links = list(dict.fromkeys(links))  # remove duplicados mantendo a ordem

    title = soup.title.get_text(strip=True) if soup.title else ""

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)

    words = list(dict.fromkeys(WORD_RE.findall(text.lower())))
    return {
        "title": title or base_url,
        "citation": text[:QUOTE_LEN],
        "words": words,
        "links": links,
    }


def fetch_and_parse(url: str, timeout: float = 10) -> dict:
    """Faz o GET e analisa. Lança exceção se falhar ou se não for HTML."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    if "html" not in resp.headers.get("Content-Type", "").lower():
        raise ValueError("conteúdo não é HTML")
    return parse_html(resp.text, resp.url)  # resp.url já considera redirects

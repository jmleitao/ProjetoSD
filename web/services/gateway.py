"""Cliente da Gateway (Meta 1) usado pelos controllers.

O servidor Web não fala com os Barrels: pede tudo à Gateway por gRPC, tal como o cliente de
consola, por isso a Web e a consola veem sempre a mesma informação.

Os controllers só conhecem `GatewayService` e `GatewayUnavailable`, nunca o gRPC. Isso mantém o
MVC limpo e permite testar os controllers com uma Gateway falsa.
"""
import os

from web.models import SearchPage, SearchResult

PAGE_SIZE = 10  # a Gateway devolve os resultados de 10 em 10


class GatewayUnavailable(Exception):
    """A Gateway não respondeu: está em baixo, não tem Barrels ativos, demorou demasiado..."""


def default_address() -> str:
    """Endereço da Gateway: variável GOOGOL_GATEWAY, ou o config.properties da Meta 1."""
    env = os.environ.get("GOOGOL_GATEWAY")
    if env:
        return env
    from googol.config import config_defaults
    return config_defaults()[0]


class GatewayService:
    def __init__(self, stub, pb, rpc_error, timeout: float = 5.0):
        """stub: o GatewayStub gerado; pb: o módulo googol_pb2; rpc_error: a classe grpc.RpcError."""
        self._stub, self._pb, self._rpc_error, self._timeout = stub, pb, rpc_error, timeout

    @classmethod
    def connect(cls, address: str = None, timeout: float = 5.0) -> "GatewayService":
        import grpc  # imports tardios: os testes não precisam de gRPC
        from googol import googol_pb2, googol_pb2_grpc
        channel = grpc.insecure_channel(address or default_address())  # o canal reconecta-se sozinho
        return cls(googol_pb2_grpc.GatewayStub(channel), googol_pb2, grpc.RpcError, timeout)

    def _call(self, method, request):
        try:
            return method(request, timeout=self._timeout)
        except self._rpc_error as e:
            code = e.code().name if hasattr(e, "code") else "ERRO"
            raise GatewayUnavailable(f"{code}: {e.details()}") from e

    def index_url(self, url: str) -> None:
        """Sugere um URL para ser indexado (vai para a frente da fila da Gateway)."""
        self._call(self._stub.putNew, self._pb.PutNewRequest(url=url))

    def search(self, terms, page: int = 0) -> SearchPage:
        """Páginas com TODOS os termos, ordenadas por ligações recebidas, 10 por página."""
        resp = self._call(self._stub.searchWords, self._pb.SearchWordsRequest(words=list(terms), page=page))
        results = [SearchResult(title=p.title, url=p.url, citation=p.citation, backlinks=p.backlinks)
                   for p in resp.pages]
        # A Gateway não preenche has_more; como o cliente de consola, assumimos que há mais
        # quando a página vem cheia.
        return SearchPage(terms=list(terms), page=page, results=results, has_more=len(results) == PAGE_SIZE)

    def backlinks(self, url: str):
        """URLs das páginas que têm uma ligação para `url`."""
        resp = self._call(self._stub.getBacklinks, self._pb.GetBacklinksRequest(url=url))
        return list(resp.urls)


_gateway = None


def get_gateway() -> GatewayService:
    """Dependência do FastAPI: uma só ligação à Gateway, criada à primeira utilização."""
    global _gateway
    if _gateway is None:
        _gateway = GatewayService.connect()
    return _gateway

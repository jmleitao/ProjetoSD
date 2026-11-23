import configparser


def config_defaults(path="config.properties"):
    """Devolve (endereço da Gateway, lista de endereços dos Barrels)."""
    cfg = configparser.ConfigParser()
    cfg.read(path)
    get = lambda k, d: cfg.get("googol", k, fallback=d)
    gateway = f"{get('gateway_host', '127.0.0.1')}:{get('gateway_port', '8183')}"
    barrels = [b.strip() for b in get("barrels", "127.0.0.1:8184,127.0.0.1:8185").split(",")]
    return gateway, barrels

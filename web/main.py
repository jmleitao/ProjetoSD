"""Servidor Web do Googol (Meta 2).

  python -m web.main                      # http://localhost:8080
  uvicorn web.main:app --port 8080

A Gateway é a da Meta 1: endereço em GOOGOL_GATEWAY (host:porta) ou no config.properties.
"""
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from web.routes import router
from web.services.gateway import GatewayUnavailable
from web.templates import BASE_DIR, render

app = FastAPI(title="Googol")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.include_router(router)


@app.exception_handler(GatewayUnavailable)
async def gateway_unavailable(request: Request, exc: GatewayUnavailable):
    """Gateway em baixo ou sem Barrels: mostra uma página de erro em vez de rebentar."""
    return render(request, "error.html", {"message": str(exc)}, status_code=503)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.main:app", host="0.0.0.0", port=8080, reload=True)
"""HTTP client helpers for INE name endpoints."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Iterable

import requests

WIDGET_URL = "https://www.ine.es/widgets/nombApell/nombApell.shtml?L=&w=1920px&h=943px&borc=000000"
GRAFICO_ENDPOINT = "https://www.ine.es/tnombres/graficoWidget"
MAPA_ENDPOINT = "https://www.ine.es/tnombres/mapaWidget"


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.ine.es",
    "Referer": WIDGET_URL,
}


def _random_session_id() -> str:
    """Generate a pseudo-random session identifier similar to the widget."""

    return str(random.randint(10**15, 10**16 - 1))


@dataclass(slots=True)
class INEClient:
    """Small helper to interact with INE endpoints using a sticky session."""

    session: requests.Session

    @classmethod
    def create(cls) -> "INEClient":
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)

        session.get(WIDGET_URL, timeout=30)

        fake_session = _random_session_id()
        session.cookies.set("rxVisitor", fake_session, domain="www.ine.es")
        session.cookies.set("rxvt", fake_session, domain="www.ine.es")

        return cls(session=session)

    def grafico_widget(self, *, nombre: str, sexo: int | str) -> dict:
        params = {"nombre": nombre, "sexo": str(sexo)}
        response = self.session.post(GRAFICO_ENDPOINT, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def mapa_widget(self, *, nombre: str, sexo: int | str, vista: str) -> dict:
        params = {"nombre": nombre, "sexo": str(sexo), "vista": vista}
        response = self.session.post(MAPA_ENDPOINT, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "INEClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


def batched(iterable: Iterable[str], size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch



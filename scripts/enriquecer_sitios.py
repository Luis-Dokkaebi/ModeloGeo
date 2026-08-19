"""
Extrae el texto de los sitios web del DENUE, fuera de línea.

Es la capa 1 del agente de prospección de la plataforma (plan §4, decisión D3).
Scrapear en tiempo real significa el vendedor esperando de 5 a 30 segundos y el
agente colgado de que un servidor ajeno responda. Los sitios son un conjunto
finito y conocido —**2,848** de los 20,957 establecimientos declararon web—, así
que se recorren aquí, una vez, y la plataforma lee texto ya extraído.

Este job vive en `ModeloGeo` y no en la plataforma por la misma razón que el
ETL: aquí sí caben `crawl4ai` y Playwright, que necesitan un binario de Chromium
que no entra en el bundle de 250 MB de una función serverless.

Uso:
    python scripts/enriquecer_sitios.py                      # los 2,848
    python scripts/enriquecer_sitios.py --limite 50          # una tanda
    python scripts/enriquecer_sitios.py --reintentar-fallos  # solo los que fallaron

Genera `data/cache_sitios.jsonl`, una línea por establecimiento:

    {"id": "9274655", "www": "WWW.TORNILLO.MX", "texto": "...",
     "error": "", "fecha": "2026-08-19T02:00:00+00:00"}

**Los fallos también se guardan, con fecha.** No es simetría: sin registrar el
fallo, cada corrida vuelve a intentar los sitios muertos —1 de cada 4 en la
medición de §2.3— y la tanda entera se va en esperar timeouts. Con el fallo
anotado, `--reintentar-fallos` es una decisión explícita y no el estado normal.

El archivo es JSONL y no JSON a propósito: se escribe línea a línea conforme
avanza, así que un job interrumpido a la mitad conserva todo lo que alcanzó.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import pandas as pd

PARQUET_POR_DEFECTO = Path("data/denue_construccion.parquet")
CACHE_POR_DEFECTO = Path("data/cache_sitios.jsonl")

TIEMPO_ESPERA_S = 15
TECHO_LECTURA_BYTES = 400 * 1024
USER_AGENT = "HoltmontProspeccion/1.0 (+https://holtmont.com)"

# Por debajo de esto el HTML llegó pero el contenido lo arma el navegador.
# `WWW.ADTEC.COM.MX` devolvió 16 caracteres en la medición de §2.3.
MINIMO_UTIL_CHARS = 120


class _TextoDeHtml(HTMLParser):
    """El texto visible. `script`, `style` y `noscript` no son contenido."""

    IGNORADAS = frozenset({"script", "style", "noscript", "template"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.partes: List[str] = []
        self._silencio = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.IGNORADAS:
            self._silencio += 1

    def handle_endtag(self, tag):
        if tag in self.IGNORADAS and self._silencio:
            self._silencio -= 1

    def handle_data(self, data):
        if not self._silencio and data.strip():
            self.partes.append(data.strip())


def texto_de_html(html: str) -> str:
    lector = _TextoDeHtml()
    try:
        lector.feed(html)
    # El HTML de la mitad de estos sitios es de 2003: reventar es lo normal.
    except Exception:
        pass
    return " ".join(" ".join(lector.partes).split())


def normalizar_url(www: Any) -> str:
    """
    `"WWW.GRUPOLAFE.COM"` -> `"https://WWW.GRUPOLAFE.COM"`.

    Solo 10 de las 2,848 URLs del DENUE traen esquema. Sin anteponerlo, `urllib`
    no sabe qué protocolo usar y falla en todas menos esas diez.
    """
    texto = str(www or "").strip()
    if not texto:
        return ""
    return texto if "://" in texto else f"https://{texto}"


def extraer_de_sitio(www: Any) -> Dict[str, str]:
    """
    `{"texto": ..., "error": ...}` de un sitio. Nunca lanza.

    Un job que recorre 2,848 servidores ajenos no puede morirse en el 300 porque
    uno devolvió un certificado vencido.
    """
    url = normalizar_url(www)
    if not url:
        return {"texto": "", "error": "sin_sitio"}
    peticion = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(peticion, timeout=TIEMPO_ESPERA_S) as respuesta:
            crudo = respuesta.read(TECHO_LECTURA_BYTES)
            codificacion = respuesta.headers.get_content_charset() or "utf-8"
        texto = texto_de_html(crudo.decode(codificacion, errors="replace"))
    except urllib.error.HTTPError as error:
        return {"texto": "", "error": f"http_{error.code}"}
    except Exception as error:
        return {"texto": "", "error": f"red: {error}"}

    if len(texto) < MINIMO_UTIL_CHARS:
        return {"texto": "", "error": "texto_insuficiente"}
    return {"texto": texto, "error": ""}


def sitios_del_parquet(parquet: Path) -> List[Dict[str, str]]:
    """
    Los establecimientos que declararon sitio web, ordenados por `id`.

    El orden fijo importa: es lo que hace que dos corridas parciales cubran el
    catálogo entero sin repetirse ni saltarse a nadie.
    """
    df = pd.read_parquet(parquet, columns=["id", "www"])
    con_sitio = df[df["www"].notna()].copy()
    con_sitio["id"] = con_sitio["id"].astype(str)
    con_sitio = con_sitio.sort_values("id")
    return [{"id": fila.id, "www": str(fila.www)} for fila in con_sitio.itertuples()]


def _leer_cache(cache: Path) -> List[Dict[str, Any]]:
    """Lo ya extraído. Una línea ilegible no invalida el archivo entero."""
    if not cache.is_file():
        return []
    filas = []
    for linea in cache.read_text(encoding="utf-8").splitlines():
        if not linea.strip():
            continue
        try:
            filas.append(json.loads(linea))
        except json.JSONDecodeError:
            continue
    return filas


def ya_resueltos(cache: Path, reintentar_fallos: bool = False) -> Set[str]:
    """
    Los `id` que no hace falta volver a pedir.

    Con `reintentar_fallos`, los que fallaron vuelven a la cola: es una decisión
    explícita, no el estado normal. Sin esto, cada corrida gastaría la mayor
    parte del tiempo esperando el timeout de los sitios muertos.
    """
    resueltos = set()
    for fila in _leer_cache(cache):
        if reintentar_fallos and fila.get("error"):
            continue
        resueltos.add(str(fila.get("id", "")))
    return resueltos


def enriquecer(parquet: Path = PARQUET_POR_DEFECTO, cache: Path = CACHE_POR_DEFECTO,
               limite: Optional[int] = None, reintentar_fallos: bool = False,
               extractor: Optional[Callable[[Any], Dict[str, str]]] = None,
               ahora: Optional[Callable[[], str]] = None) -> int:
    """
    Recorre los sitios pendientes y los añade al archivo. Devuelve cuántos.

    `extractor` y `ahora` se inyectan para que las pruebas recorran el job
    completo sin salir a la red y con una fecha estable.
    """
    leer = extractor or extraer_de_sitio
    reloj = ahora or (lambda: datetime.now(timezone.utc).isoformat())

    pendientes = [s for s in sitios_del_parquet(parquet)
                  if s["id"] not in ya_resueltos(cache, reintentar_fallos)]
    if limite is not None:
        pendientes = pendientes[:limite]

    cache.parent.mkdir(parents=True, exist_ok=True)
    escritos = 0
    # Se abre en modo append y se vacía cada línea: un job interrumpido a la
    # mitad —y este tarda horas— conserva todo lo que alcanzó a leer.
    with cache.open("a", encoding="utf-8") as salida:
        for sitio in pendientes:
            resultado = leer(sitio["www"])
            salida.write(json.dumps({
                "id": sitio["id"], "www": sitio["www"],
                "texto": resultado.get("texto", ""),
                "error": resultado.get("error", ""),
                "fecha": reloj(),
            }, ensure_ascii=False) + "\n")
            salida.flush()
            escritos += 1
            detalle = resultado.get("error") or f"{len(resultado.get('texto', ''))} chars"
            print(f"  [{escritos:>5}/{len(pendientes)}] {sitio['www']:<40} {detalle}")
    return escritos


def resumen(cache: Path = CACHE_POR_DEFECTO) -> Dict[str, int]:
    """Cuántos sitios dieron texto y cuántos no. Es el número que interesa medir."""
    filas = _leer_cache(cache)
    con_texto = sum(1 for f in filas if f.get("texto"))
    return {"total": len(filas), "con_texto": con_texto,
            "con_error": len(filas) - con_texto}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, default=PARQUET_POR_DEFECTO)
    parser.add_argument("--cache", type=Path, default=CACHE_POR_DEFECTO)
    parser.add_argument("--limite", type=int, default=None,
                        help="cuántos sitios recorrer en esta tanda")
    parser.add_argument("--reintentar-fallos", action="store_true",
                        help="vuelve a intentar los que quedaron con error")
    args = parser.parse_args()

    if not args.parquet.is_file():
        print(f"No existe el archivo de entrada: {args.parquet}", file=sys.stderr)
        return 1

    escritos = enriquecer(args.parquet, args.cache, args.limite, args.reintentar_fallos)
    cuentas = resumen(args.cache)
    print(f"\n{escritos:,} sitios nuevos en {args.cache}")
    print(f"  acumulado: {cuentas['total']:,} sitios, "
          f"{cuentas['con_texto']:,} con texto, {cuentas['con_error']:,} sin él")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

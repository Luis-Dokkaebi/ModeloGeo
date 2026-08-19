"""
Pruebas del enriquecimiento de sitios (`scripts/enriquecer_sitios.py`).

Este job recorre 2,848 servidores ajenos y tarda horas. Lo que se prueba aquí no
es que sepa leer HTML —eso lo cubren dos casos— sino las tres propiedades que
deciden si el job se puede correr de verdad:

1. **Es reanudable.** Se interrumpe, se vuelve a lanzar y sigue donde iba. Sin
   eso, cualquier corte a mitad obliga a empezar de cero.
2. **Guarda los fallos.** 1 de cada 4 sitios del DENUE falló en la medición de
   §2.3. Si el fallo no se anota, cada corrida gasta la mayor parte del tiempo
   esperando el timeout de los mismos sitios muertos.
3. **No se muere por un sitio.** Un certificado vencido en el número 300 no
   puede tirar las 2,548 lecturas que faltan.

Todo con un extractor inyectado: la suite no sale a la red.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pandas as pd
import pytest

RAIZ = pathlib.Path(__file__).resolve().parent.parent


def _cargar_modulo():
    """El script vive en `scripts/`, que no es un paquete importable."""
    ruta = RAIZ / "scripts" / "enriquecer_sitios.py"
    spec = importlib.util.spec_from_file_location("enriquecer_sitios", ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


job = _cargar_modulo()

FECHA = "2026-08-19T00:00:00+00:00"

# Tres con sitio y uno sin él, como el archivo real: solo 2,848 de 20,957
# establecimientos declararon web.
SITIOS = [
    ("03", "WWW.NORSK.MX"),
    ("01", "WWW.TORNILLO.MX"),
    ("02", "WWW.CROMA.MX"),
    ("04", None),
]


@pytest.fixture
def parquet(tmp_path: pathlib.Path) -> pathlib.Path:
    ruta = tmp_path / "denue_construccion.parquet"
    pd.DataFrame({"id": [i for i, _ in SITIOS],
                  "www": [w for _, w in SITIOS]}).to_parquet(ruta, index=False)
    return ruta


@pytest.fixture
def cache(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "cache_sitios.jsonl"


def _extractor(respuestas):
    """Extractor falso. Registra a quién se le pidió, en orden."""
    pedidos = []

    def leer(www):
        pedidos.append(www)
        return respuestas.get(www, {"texto": "", "error": "red: sin respuesta"})

    leer.pedidos = pedidos
    return leer


def _lineas(cache: pathlib.Path):
    return [json.loads(linea) for linea in cache.read_text(encoding="utf-8").splitlines()
            if linea.strip()]


# ----------------------------------------------------------------------
# La cola de trabajo
# ----------------------------------------------------------------------

def test_solo_se_visitan_los_establecimientos_que_declararon_sitio(parquet):
    """Pedirle la web a quien no la declaró es gastar un timeout por nada."""
    assert [s["id"] for s in job.sitios_del_parquet(parquet)] == ["01", "02", "03"]


def test_la_cola_va_ordenada_por_id(parquet):
    """
    El orden fijo es lo que hace que dos tandas parciales cubran el catálogo
    entero sin repetir a nadie ni saltarse a nadie.
    """
    ids = [s["id"] for s in job.sitios_del_parquet(parquet)]
    assert ids == sorted(ids)


def test_una_tanda_con_limite_no_recorre_mas_de_lo_pedido(parquet, cache):
    leer = _extractor({})
    assert job.enriquecer(parquet, cache, limite=2, extractor=leer, ahora=lambda: FECHA) == 2
    assert len(leer.pedidos) == 2


def test_el_job_es_reanudable(parquet, cache):
    """
    Se interrumpe, se vuelve a lanzar y sigue donde iba. Tarda horas: sin esto,
    cualquier corte a mitad obliga a empezar de cero.
    """
    leer = _extractor({w: {"texto": "T" * 200, "error": ""} for _, w in SITIOS if w})
    job.enriquecer(parquet, cache, limite=2, extractor=leer, ahora=lambda: FECHA)
    primera_tanda = list(leer.pedidos)

    segunda = _extractor({w: {"texto": "T" * 200, "error": ""} for _, w in SITIOS if w})
    job.enriquecer(parquet, cache, extractor=segunda, ahora=lambda: FECHA)

    assert set(primera_tanda) & set(segunda.pedidos) == set(), "se repitió un sitio"
    assert len(_lineas(cache)) == 3


# ----------------------------------------------------------------------
# Los fallos
# ----------------------------------------------------------------------

def test_los_fallos_se_guardan_con_su_fecha(parquet, cache):
    """
    Sin registrar el fallo, cada corrida vuelve a intentar los sitios muertos
    —1 de cada 4— y la tanda entera se va en esperar timeouts.
    """
    job.enriquecer(parquet, cache, extractor=_extractor({}), ahora=lambda: FECHA)
    filas = _lineas(cache)
    assert len(filas) == 3
    assert all(f["error"] for f in filas)
    assert all(f["fecha"] == FECHA for f in filas)


def test_un_sitio_ya_fallado_no_se_reintenta_solo(parquet, cache):
    job.enriquecer(parquet, cache, extractor=_extractor({}), ahora=lambda: FECHA)
    segunda = _extractor({})
    assert job.enriquecer(parquet, cache, extractor=segunda, ahora=lambda: FECHA) == 0
    assert segunda.pedidos == []


def test_reintentar_fallos_es_una_decision_explicita(parquet, cache):
    job.enriquecer(parquet, cache, extractor=_extractor({}), ahora=lambda: FECHA)
    segunda = _extractor({"WWW.TORNILLO.MX": {"texto": "T" * 200, "error": ""}})
    escritos = job.enriquecer(parquet, cache, reintentar_fallos=True,
                              extractor=segunda, ahora=lambda: FECHA)
    assert escritos == 3
    assert sorted(segunda.pedidos) == ["WWW.CROMA.MX", "WWW.NORSK.MX", "WWW.TORNILLO.MX"]


def test_lo_que_ya_dio_texto_no_se_vuelve_a_pedir_ni_reintentando(parquet, cache):
    """Reintentar los fallos no puede convertirse en rehacer el trabajo bueno."""
    job.enriquecer(parquet, cache, limite=1, ahora=lambda: FECHA,
                   extractor=_extractor({"WWW.TORNILLO.MX": {"texto": "T" * 200, "error": ""}}))
    segunda = _extractor({})
    job.enriquecer(parquet, cache, reintentar_fallos=True, extractor=segunda,
                   ahora=lambda: FECHA)
    assert "WWW.TORNILLO.MX" not in segunda.pedidos


def test_una_linea_corrupta_no_invalida_el_archivo_entero(cache):
    """
    El archivo se escribe línea a línea durante horas. Un corte a media línea es
    un final plausible, y no puede costar las 2,847 anteriores.
    """
    cache.write_text(
        json.dumps({"id": "01", "texto": "algo", "error": ""}) + "\n"
        + '{"id": "02", "texto": "cor\n',
        encoding="utf-8")
    assert job.ya_resueltos(cache) == {"01"}


# ----------------------------------------------------------------------
# La lectura del HTML
# ----------------------------------------------------------------------

def test_el_texto_extraido_no_incluye_scripts_ni_estilos():
    html = ("<html><head><style>.a{color:red}</style></head><body>"
            "<script>window.x=1</script><p>Venta de tubería de cobre</p></body></html>")
    texto = job.texto_de_html(html)
    assert "window.x" not in texto and "color:red" not in texto
    assert "Venta de tubería de cobre" in texto


@pytest.mark.parametrize("crudo,esperado", [
    ("WWW.GRUPOLAFE.COM", "https://WWW.GRUPOLAFE.COM"),
    ("http://ya.tiene.mx", "http://ya.tiene.mx"),
    ("", ""),
    (None, ""),
])
def test_se_antepone_el_esquema_que_el_denue_no_trae(crudo, esperado):
    """Solo 10 de las 2,848 URLs del DENUE traen esquema."""
    assert job.normalizar_url(crudo) == esperado


def test_un_establecimiento_sin_sitio_no_sale_a_la_red():
    assert job.extraer_de_sitio(None) == {"texto": "", "error": "sin_sitio"}


def test_el_resumen_cuenta_lo_que_sirvio_y_lo_que_no(parquet, cache):
    """Es el número que interesa medir al terminar la corrida."""
    job.enriquecer(parquet, cache, ahora=lambda: FECHA,
                   extractor=_extractor({"WWW.TORNILLO.MX": {"texto": "T" * 200, "error": ""}}))
    assert job.resumen(cache) == {"total": 3, "con_texto": 1, "con_error": 2}


def test_el_resumen_de_un_archivo_que_no_existe_es_cero(tmp_path):
    assert job.resumen(tmp_path / "no_existe.jsonl") == {
        "total": 0, "con_texto": 0, "con_error": 0}


def test_falla_con_mensaje_util_si_no_existe_la_entrada(tmp_path, capsys):
    import sys

    argv = sys.argv
    sys.argv = ["enriquecer_sitios.py", "--parquet", str(tmp_path / "no_existe.parquet")]
    try:
        assert job.main() == 1
    finally:
        sys.argv = argv
    assert "No existe el archivo de entrada" in capsys.readouterr().err

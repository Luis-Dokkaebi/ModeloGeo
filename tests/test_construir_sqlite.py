"""
Pruebas de la construcción del SQLite del catálogo (`scripts/construir_sqlite.py`).

Este archivo prueba contra el Parquet **real** del repositorio, no contra un
doble. Es deliberado y es la diferencia con `test_convertir_denue_a_parquet.py`:
allá lo que se probaba era la lectura de un CSV que no se versiona, así que hacía
falta fabricar uno; aquí la entrada está en `data/` y las cifras que interesan
—20,957 establecimientos, 1,972 proveedores de 11+ personas con contacto— son
cifras del negocio, medidas, no inventadas. Fijarlas contra un doble de tres
filas no probaría nada.

Lo que se protege:

1. **No se pierde ni una fila.** El SQLite es el artefacto que consume la
   plataforma; si llega corto, el mapa muestra menos negocios de los que existen
   y nadie lo nota.
2. **`cod_postal` sigue siendo texto.** La trampa clásica del INEGI: "01000"
   guardado como entero se vuelve 1000 y ningún cruce empareja. Falla en
   silencio.
3. **Los tres índices existen y se usan.** Un índice que el planificador ignora
   es peso muerto en el bundle; se verifica con `EXPLAIN QUERY PLAN`, que es la
   única forma de saberlo.
4. **El archivo es determinista.** R8: dos corridas sobre el mismo Parquet
   producen el mismo archivo, byte por byte. Es lo que permite versionar la
   salida y revisar su diff.
5. **El archivo cabe.** El bundle de Vercel tiene 250 MB para todo; el catálogo
   se queda muy por debajo de su parte.
"""

from __future__ import annotations

import hashlib
import importlib.util
import pathlib
import sqlite3

import pandas as pd
import pytest

RAIZ = pathlib.Path(__file__).resolve().parent.parent
PARQUET = RAIZ / "data" / "denue_construccion.parquet"

# Cifras medidas sobre `denue_construccion.parquet`, documentadas en el README
# del repositorio y en §2 del plan de integración.
FILAS_ESPERADAS = 20_957
PROVEEDORES_11_MAS_CON_CONTACTO = 1_972
PESO_MAXIMO_BYTES = 8 * 1024 * 1024

# Los rangos de `per_ocu` que el DENUE usa para 11 personas o más. El campo es
# texto libre con rangos, no un número: no hay `per_ocu >= 11` que valga.
RANGOS_11_MAS = (
    "11 a 30 personas", "31 a 50 personas", "51 a 100 personas",
    "101 a 250 personas", "251 y más personas",
)


def _cargar_modulo():
    """El script vive en `scripts/`, que no es un paquete importable."""
    ruta = RAIZ / "scripts" / "construir_sqlite.py"
    spec = importlib.util.spec_from_file_location("construir_sqlite", ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


constructor = _cargar_modulo()


@pytest.fixture(scope="module")
def sqlite_real(tmp_path_factory) -> pathlib.Path:
    """
    Construye el SQLite desde el Parquet real, una sola vez para todo el módulo.

    Se escribe en `tmp_path`, no en `data/`: una prueba no toca el artefacto
    versionado.
    """
    salida = tmp_path_factory.mktemp("catalogo") / "denue.sqlite"
    constructor.construir(PARQUET, salida)
    return salida


@pytest.fixture(scope="module")
def conexion(sqlite_real: pathlib.Path):
    """Conexión de solo lectura, igual que la abrirá la plataforma."""
    con = sqlite3.connect(f"file:{sqlite_real}?mode=ro", uri=True)
    yield con
    con.close()


def test_llegan_las_20957_filas_completas(conexion):
    """El artefacto es el catálogo entero, no una muestra."""
    (filas,) = conexion.execute("SELECT COUNT(*) FROM denue").fetchone()
    assert filas == FILAS_ESPERADAS


def test_el_esquema_es_el_de_catorce_columnas(conexion):
    """
    De las 42 columnas del DENUE bajan 14. Ni una más —cada columna extra es
    peso en el bundle— ni una menos —falta un dato que la ficha necesita—.
    """
    columnas = [fila[1] for fila in conexion.execute("PRAGMA table_info(denue)")]
    assert columnas == [
        "id", "nom_estab", "nombre_act", "codigo_act", "per_ocu", "telefono",
        "correoelec", "www", "municipio", "nom_vial", "numero_ext", "cod_postal",
        "latitud", "longitud",
    ]


def test_cod_postal_conserva_los_ceros_a_la_izquierda(conexion):
    """
    La patología clásica del INEGI. `"01000"` guardado como entero se vuelve
    `1000`: no revienta nada, simplemente deja de cruzar contra cualquier otra
    fuente del instituto.
    """
    filas = conexion.execute(
        "SELECT typeof(cod_postal), cod_postal FROM denue "
        "WHERE cod_postal LIKE '0%' ORDER BY cod_postal LIMIT 5"
    ).fetchall()
    assert filas, "el DENUE de CDMX tiene códigos postales que empiezan por cero"
    assert all(tipo == "text" for tipo, _ in filas)
    assert all(valor.startswith("0") and len(valor) == 5 for _, valor in filas)

    # Y el caso puntual que documenta el README de `data/`: 15700, sin cero
    # delante, tiene que seguir siendo la cadena "15700" y no el entero 15700.
    tipo, valor = conexion.execute(
        "SELECT typeof(cod_postal), cod_postal FROM denue WHERE cod_postal = '15700' LIMIT 1"
    ).fetchone()
    assert (tipo, valor) == ("text", "15700")


def test_los_tres_indices_existen(conexion):
    """Alcaldía, giro y coordenadas: los tres accesos que hace la plataforma."""
    indices = {
        fila[0] for fila in conexion.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'denue'"
        )
    }
    assert {"idx_mun", "idx_act", "idx_geo"} <= indices


def test_la_consulta_por_alcaldia_usa_el_indice(conexion):
    """
    Que el índice exista no significa que el planificador lo elija. `EXPLAIN
    QUERY PLAN` es la única forma de saberlo, y sin él la consulta por alcaldía
    recorre las 20,957 filas.
    """
    plan = conexion.execute(
        "EXPLAIN QUERY PLAN SELECT id FROM denue WHERE municipio = ?", ("Cuauhtémoc",)
    ).fetchall()
    detalle = " ".join(fila[3] for fila in plan)
    assert "idx_mun" in detalle
    assert "SCAN denue" not in detalle


def test_la_consulta_por_giro_usa_el_indice(conexion):
    """El filtro por giro del notebook, traducido a la plataforma."""
    plan = conexion.execute(
        "EXPLAIN QUERY PLAN SELECT id FROM denue WHERE codigo_act = ?", ("467111",)
    ).fetchall()
    detalle = " ".join(fila[3] for fila in plan)
    assert "idx_act" in detalle
    assert "SCAN denue" not in detalle


def test_el_bbox_del_mapa_usa_el_indice_geografico(conexion):
    """
    El mapa pide lo que cabe en la vista actual. Sin `idx_geo` cada paneo son
    20,957 filas leídas.
    """
    plan = conexion.execute(
        "EXPLAIN QUERY PLAN SELECT id FROM denue "
        "WHERE latitud BETWEEN ? AND ? AND longitud BETWEEN ? AND ?",
        (19.3, 19.5, -99.2, -99.0),
    ).fetchall()
    detalle = " ".join(fila[3] for fila in plan)
    assert "idx_geo" in detalle
    assert "SCAN denue" not in detalle


def test_los_proveedores_de_11_o_mas_con_contacto_son_1972(conexion):
    """
    La cifra que publica el README del repositorio, recalculada desde el SQLite:
    la lista que un equipo de compras puede recorrer de verdad. Si esta consulta
    deja de dar 1,972 es que se perdieron filas, o que un faltante dejó de ser
    NULL y `telefono IS NOT NULL` empezó a contar de más.
    """
    marcadores = ", ".join("?" * len(RANGOS_11_MAS))
    (total,) = conexion.execute(
        f"SELECT COUNT(*) FROM denue WHERE per_ocu IN ({marcadores}) "
        "AND (telefono IS NOT NULL OR correoelec IS NOT NULL OR www IS NOT NULL)",
        RANGOS_11_MAS,
    ).fetchone()
    assert total == PROVEEDORES_11_MAS_CON_CONTACTO


def test_los_faltantes_llegan_como_null(conexion):
    """
    El Parquet marca los faltantes con `NaN`. Escrito tal cual, `NaN` acaba en
    una columna TEXT como flotante y `telefono IS NULL` deja de significar "no
    declaró teléfono" — que es justo la condición del conteo de arriba.
    """
    (sin_telefono,) = conexion.execute(
        "SELECT COUNT(*) FROM denue WHERE telefono IS NULL"
    ).fetchone()
    assert sin_telefono == 12_787
    tipos = {
        fila[0] for fila in conexion.execute(
            "SELECT DISTINCT typeof(telefono) FROM denue"
        )
    }
    assert tipos == {"text", "null"}


def test_el_archivo_pesa_menos_de_ocho_megas(sqlite_real: pathlib.Path):
    """
    El bundle de una función serverless de Vercel tiene 250 MB para todo, y el
    catálogo es solo una parte. La medición de referencia fue de ~5.3 MB; el
    techo deja margen para que crezca el DENUE sin que la prueba mienta.
    """
    peso = sqlite_real.stat().st_size
    assert peso < PESO_MAXIMO_BYTES, f"el catálogo pesa {peso:,} bytes"


def test_dos_corridas_producen_el_mismo_archivo(tmp_path: pathlib.Path):
    """
    R8. El SQLite se versiona: si dos construcciones del mismo Parquet dieran
    bytes distintos, cada regeneración ensuciaría el diff con 5 MB de ruido y
    nadie podría revisar qué cambió de verdad.
    """
    primera = tmp_path / "primera.sqlite"
    segunda = tmp_path / "segunda.sqlite"
    constructor.construir(PARQUET, primera)
    constructor.construir(PARQUET, segunda)
    assert _sha256(primera) == _sha256(segunda)


def test_reconstruir_encima_de_una_corrida_previa_da_el_mismo_archivo(tmp_path: pathlib.Path):
    """
    Continuación de R8, y el caso real: se regenera sobre el archivo que ya está
    en `data/`. SQLite reusa las páginas libres del archivo que encuentra, así
    que si el script no borrara la salida antes, esta prueba fallaría.
    """
    salida = tmp_path / "denue.sqlite"
    constructor.construir(PARQUET, salida)
    primera = _sha256(salida)
    constructor.construir(PARQUET, salida)
    assert _sha256(salida) == primera


def test_rechaza_un_parquet_al_que_le_faltan_columnas(tmp_path: pathlib.Path):
    """
    Si el INEGI cambia el formato, el ETL tiene que decirlo con el nombre de la
    columna que falta, no escribir un catálogo cojo.
    """
    incompleto = tmp_path / "incompleto.parquet"
    pd.DataFrame({"id": ["1"], "nom_estab": ["FERRETERIA EL TORNILLO"]}).to_parquet(incompleto)
    with pytest.raises(ValueError, match="cod_postal"):
        constructor.construir(incompleto, tmp_path / "salida.sqlite")


def test_falla_con_mensaje_util_si_no_existe_la_entrada(tmp_path, capsys):
    """
    Un ETL que se corre a mano cada varios meses tiene que decir qué pasó. El
    modo de fallo más probable es teclear mal la ruta del Parquet.
    """
    import sys

    argv = sys.argv
    sys.argv = ["construir_sqlite.py", str(tmp_path / "no_existe.parquet")]
    try:
        assert constructor.main() == 1
    finally:
        sys.argv = argv
    assert "No existe el archivo de entrada" in capsys.readouterr().err


def _sha256(ruta: pathlib.Path) -> str:
    return hashlib.sha256(ruta.read_bytes()).hexdigest()

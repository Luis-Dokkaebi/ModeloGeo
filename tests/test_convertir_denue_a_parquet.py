"""
Pruebas de la conversión del DENUE a Parquet (`scripts/convertir_denue_a_parquet.py`).

Cada caso viene de una patología real del archivo del INEGI, no de una
hipótesis. El CSV de origen pesa 248 MB y trae 462,732 establecimientos de la
Ciudad de México; lo que se prueba aquí es que pasarlo a Parquet **no pierde ni
deforma nada**, porque el archivo convertido es el que se versiona y el crudo no.

Las tres patologías observadas al convertir el archivo real:

1. **Ceros a la izquierda.** `cve_ent` vale `"09"` (Ciudad de México),
   `cve_mun` `"017"` y `cod_postal` `"15700"`. Leer el CSV sin forzar texto los
   vuelve enteros, y `9` ya no cruza contra ninguna otra fuente del INEGI. Falla
   en silencio: nada revienta, simplemente ningún registro empareja.
2. **Encoding latin-1.** El INEGI no publica en UTF-8. Leerlo como UTF-8 rompe
   `Ciudad de México` y `Servicios de ingeniería`.
3. **Coordenadas como texto.** `latitud` y `longitud` vienen entrecomilladas
   como el resto; son el eje de todo el análisis espacial y tienen que salir
   numéricas.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pandas as pd
import pytest

RAIZ = pathlib.Path(__file__).resolve().parent.parent


def _cargar_modulo():
    """El script vive en `scripts/`, que no es un paquete importable."""
    ruta = RAIZ / "scripts" / "convertir_denue_a_parquet.py"
    spec = importlib.util.spec_from_file_location("convertir_denue_a_parquet", ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


convertidor = _cargar_modulo()

# Columnas del DENUE que intervienen en las tres patologías, más las que separan
# un giro de construcción de uno que no lo es.
_ENCABEZADO = (
    "id,clee,nom_estab,codigo_act,nombre_act,per_ocu,cod_postal,"
    "cve_ent,entidad,cve_mun,municipio,ageb,telefono,correoelec,www,"
    "tipoUniEco,latitud,longitud,fecha_alta"
)

_FILAS = (
    # Una ferretería: giro de construcción, con acento en la alcaldía.
    '"1","09017A","FERRETERIA EL TORNILLO","467111",'
    '"Comercio al por menor en ferreterías y tlapalerías","0 a 5 personas","15700",'
    '"09","Ciudad de México","017","Cuauhtémoc","1204","5512345678",'
    '"VENTAS@TORNILLO.MX","www.tornillo.mx","Fijo","19.41887981","-99.09012648","2026-04"',
    # Servicios de ingeniería: giro de construcción, otra categoría.
    '"2","09005B","INGENIERIA DEL VALLE","541330","Servicios de ingeniería",'
    '"11 a 30 personas","03100","09","Ciudad de México","005","Benito Juárez",'
    '"0870",,"CONTACTO@IDV.MX",,"Fijo","19.38000000","-99.16000000","2026-04"',
    # Una taquería: NO es construcción. Debe quedar en el archivo completo y
    # desaparecer del subconjunto.
    '"3","09015C","TAQUERIA LA ESQUINA","722513","Restaurantes de tacos y tortas",'
    '"0 a 5 personas","06700","09","Ciudad de México","015","Cuauhtémoc",'
    '"1101",,,,"Fijo","19.42000000","-99.15000000","2026-04"',
)


@pytest.fixture
def csv_denue(tmp_path: pathlib.Path) -> pathlib.Path:
    """CSV mínimo con el formato exacto del INEGI: latin-1 y todo entrecomillado."""
    ruta = tmp_path / "denue_inegi_09_.csv"
    contenido = _ENCABEZADO + "\n" + "\n".join(_FILAS) + "\n"
    ruta.write_text(contenido, encoding="latin-1")
    return ruta


@pytest.fixture
def convertido(csv_denue: pathlib.Path, tmp_path: pathlib.Path):
    """Corre la conversión y devuelve los dos DataFrames resultantes."""
    salida = tmp_path / "data"
    convertidor.convertir(csv_denue, salida)
    return (
        pd.read_parquet(salida / "denue_cdmx.parquet"),
        pd.read_parquet(salida / "denue_construccion.parquet"),
    )


def test_conserva_todas_las_filas_y_columnas(convertido):
    """El archivo completo es el CSV entero: convertir no es filtrar."""
    completo, _ = convertido
    assert len(completo) == 3
    assert len(completo.columns) == _ENCABEZADO.count(",") + 1


def test_preserva_ceros_a_la_izquierda(convertido):
    """
    Patología 1. `"09"` que se vuelve `9` deja de cruzar contra cualquier otra
    fuente del INEGI, y lo hace sin lanzar un solo error.
    """
    completo, _ = convertido
    assert list(completo["cve_ent"].astype(str)) == ["09", "09", "09"]
    assert list(completo["cve_mun"].astype(str)) == ["017", "005", "015"]
    assert completo["cod_postal"].astype(str).iloc[0] == "15700"
    assert completo["ageb"].astype(str).iloc[1] == "0870"


def test_preserva_acentos_del_encoding_latin1(convertido):
    """Patología 2. El INEGI no publica en UTF-8."""
    completo, _ = convertido
    assert completo["entidad"].astype(str).iloc[0] == "Ciudad de México"
    assert completo["municipio"].astype(str).iloc[0] == "Cuauhtémoc"
    assert completo["nombre_act"].astype(str).iloc[1] == "Servicios de ingeniería"


def test_coordenadas_quedan_numericas_y_exactas(convertido):
    """
    Patología 3. Vienen como texto y son el eje del análisis espacial. La
    igualdad es exacta a propósito: una conversión que redondee mueve el
    establecimiento de manzana.
    """
    completo, _ = convertido
    assert pd.api.types.is_numeric_dtype(completo["latitud"])
    assert pd.api.types.is_numeric_dtype(completo["longitud"])
    assert completo["latitud"].iloc[0] == 19.41887981
    assert completo["longitud"].iloc[0] == -99.09012648


def test_subconjunto_deja_solo_giros_de_construccion(convertido):
    """La taquería sobrevive en el archivo completo y no en el de construcción."""
    completo, construccion = convertido
    assert len(construccion) == 2
    assert set(construccion["nom_estab"]) == {"FERRETERIA EL TORNILLO", "INGENIERIA DEL VALLE"}
    assert "TAQUERIA LA ESQUINA" in set(completo["nom_estab"])


def test_subconjunto_poda_las_categorias_que_ya_no_usa(convertido):
    """
    Un subconjunto hereda el diccionario del padre. Sin podarlo, el archivo de
    construcción seguiría cargando los giros que ya no contiene —en el archivo
    real, mil categorías de las que solo usa 99.
    """
    _, construccion = convertido
    giros = construccion["nombre_act"]
    assert str(giros.dtype) == "category"
    assert "Restaurantes de tacos y tortas" not in list(giros.cat.categories)


def test_el_catalogo_de_giros_no_tiene_duplicados():
    """
    Son 99 giros curados a mano. Un duplicado no rompe el filtro (`isin` lo
    absorbe) pero sí delata una edición descuidada del catálogo.
    """
    giros = convertidor.GIROS_CONSTRUCCION
    assert len(giros) == len(set(giros))
    assert len(giros) == 99


def test_falla_con_mensaje_util_si_no_existe_la_entrada(tmp_path, capsys):
    """
    Un ETL que se corre a mano cada varios meses tiene que decir qué pasó. El
    modo de fallo más probable es teclear mal la ruta del CSV.
    """
    import sys

    argv = sys.argv
    sys.argv = ["convertir_denue_a_parquet.py", str(tmp_path / "no_existe.csv")]
    try:
        assert convertidor.main() == 1
    finally:
        sys.argv = argv
    assert "No existe el archivo de entrada" in capsys.readouterr().err

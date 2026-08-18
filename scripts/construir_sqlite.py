"""
Construye el SQLite del catálogo a partir del Parquet de construcción.

Es el segundo eslabón del ETL: `csv → parquet` lo hace
`convertir_denue_a_parquet.py`, y `parquet → sqlite` lo hace este script. El
Parquet es el formato de trabajo del laboratorio; el SQLite es el artefacto que
consume la plataforma.

Por qué SQLite y no el Parquet directamente: leerlo exige `pandas` + `pyarrow` +
`numpy`, que descomprimidos suman 251 MB y no caben en el límite de 250 MB de una
función serverless de Vercel. El SQLite se consulta con el módulo `sqlite3` de la
biblioteca estándar, así que la plataforma no gana ni una dependencia. Ese es el
punto de toda la arquitectura: `pandas` y `pyarrow` se quedan aquí.

De las 42 columnas del DENUE bajan 14 —las que pinta el mapa y las que forman la
ficha del negocio— y se crean 3 índices: alcaldía, giro y coordenadas. El
resultado son ~5 MB y consultas de 3 a 14 ms.

`cod_postal` viaja como TEXT a propósito. Los ceros a la izquierda del INEGI
("09" para la entidad, "017" para el municipio, "15700" para el código postal)
son la trampa clásica de estos datos: si `cod_postal` se guardara como entero,
"01000" se volvería 1000 y ningún cruce posterior emparejaría. Falla en silencio.
El Parquet ya los preserva; aquí se preservan también.

Uso:
    python scripts/construir_sqlite.py

    python scripts/construir_sqlite.py data/denue_construccion.parquet data/denue.sqlite

Requiere `pandas` y `pyarrow`, que no están en los requirements de la
plataforma: esto es una herramienta de preparación de datos, se corre a mano
cuando el INEGI publica una actualización, y su salida es lo único que se
versiona.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

PARQUET_POR_DEFECTO = Path("data/denue_construccion.parquet")
SQLITE_POR_DEFECTO = Path("data/denue.sqlite")

# Las 14 columnas que bajan del DENUE, en el orden del esquema. Las otras 28 no
# viajan: el mapa no las pinta y cada una que sobra es peso en el bundle.
COLUMNAS_TEXTO = (
    "id", "nom_estab", "nombre_act", "codigo_act", "per_ocu", "telefono",
    "correoelec", "www", "municipio", "nom_vial", "numero_ext", "cod_postal",
)
COLUMNAS_REALES = ("latitud", "longitud")
COLUMNAS = COLUMNAS_TEXTO + COLUMNAS_REALES

# `id` es la clave del DENUE y es única; declararla PRIMARY KEY deja que la
# plataforma resuelva la ficha de un establecimiento por búsqueda de índice.
TABLA = """
CREATE TABLE denue (
  id TEXT PRIMARY KEY, nom_estab TEXT, nombre_act TEXT, codigo_act TEXT,
  per_ocu TEXT, telefono TEXT, correoelec TEXT, www TEXT,
  municipio TEXT, nom_vial TEXT, numero_ext TEXT, cod_postal TEXT,
  latitud REAL, longitud REAL
);
"""

# Los tres accesos que hace la plataforma: filtro por alcaldía, filtro por giro y
# bbox del mapa. Se crean *después* de insertar: construir un índice de una vez
# sobre la tabla llena lo deja compacto, mientras que mantenerlo actualizado
# durante 20,957 inserciones fragmenta su b-tree y engorda el archivo medio mega.
INDICES = """
CREATE INDEX idx_mun ON denue(municipio);
CREATE INDEX idx_act ON denue(codigo_act);
CREATE INDEX idx_geo ON denue(latitud, longitud);
"""

INSERCION = "INSERT INTO denue VALUES (" + ", ".join("?" * len(COLUMNAS)) + ")"


def _filas(df: pd.DataFrame) -> list[tuple]:
    """
    Convierte el DataFrame en las tuplas que espera `executemany`.

    Dos conversiones, ninguna cosmética:

    - Las columnas categóricas del Parquet (`nombre_act`, `codigo_act`,
      `per_ocu`, `municipio`) son diccionarios comprimidos, no texto. `sqlite3`
      no sabe qué hacer con ellas y hay que aplanarlas.
    - Los faltantes del Parquet son `NaN`, que `sqlite3` guardaría como el flotante
      NaN dentro de una columna TEXT. Se pasan a `None` para que lleguen como NULL
      y `telefono IS NULL` signifique lo que dice.
    """
    tabla = df.loc[:, list(COLUMNAS)].copy()
    for columna in COLUMNAS_TEXTO:
        serie = tabla[columna].astype(object)
        tabla[columna] = serie.where(serie.notna(), None)
    return list(tabla.itertuples(index=False, name=None))


def construir(parquet: Path, salida: Path) -> int:
    """
    Lee el Parquet y escribe el SQLite en `salida`. Devuelve las filas escritas.

    La salida se borra antes de escribir. No es higiene: SQLite reusa las páginas
    libres del archivo que encuentra, así que construir encima de una corrida
    anterior produce bytes distintos con el mismo contenido y rompe R8 —dos
    corridas sobre el mismo Parquet tienen que producir el mismo archivo—.
    """
    df = pd.read_parquet(parquet)
    faltantes = [columna for columna in COLUMNAS if columna not in df.columns]
    if faltantes:
        raise ValueError(f"Al Parquet le faltan columnas del esquema: {', '.join(faltantes)}")

    print(f"Leídas {len(df):,} filas × {len(df.columns)} columnas de {parquet.name}")

    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.unlink(missing_ok=True)

    filas = _filas(df)
    with sqlite3.connect(salida) as conexion:
        conexion.executescript(TABLA)
        conexion.executemany(INSERCION, filas)
        conexion.executescript(INDICES)
    conexion.close()

    print(f"  {salida.name:28} {len(filas):>7,} filas  {salida.stat().st_size / 1e6:6.2f} MB")
    return len(filas)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("entrada", type=Path, nargs="?", default=PARQUET_POR_DEFECTO,
                        help=f"Parquet de construcción (por defecto: {PARQUET_POR_DEFECTO})")
    parser.add_argument("salida", type=Path, nargs="?", default=SQLITE_POR_DEFECTO,
                        help=f"SQLite a escribir (por defecto: {SQLITE_POR_DEFECTO})")
    args = parser.parse_args()

    if not args.entrada.is_file():
        print(f"No existe el archivo de entrada: {args.entrada}", file=sys.stderr)
        return 1

    construir(args.entrada, args.salida)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

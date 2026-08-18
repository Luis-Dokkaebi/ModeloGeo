"""
Convierte el CSV del DENUE (INEGI) a Parquet.

El CSV que publica INEGI para la Ciudad de México pesa 248 MB: demasiado para
versionarlo en el repositorio (GitHub avisa a partir de 50 MB y rechaza a los
100 MB). En Parquet baja a 28 MB sin perder una sola celda, y entra sin Git LFS.

La reducción no viene de tirar datos sino de dos propiedades del formato:
almacenamiento por columna —que comprime bien porque los valores de una misma
columna se parecen entre sí— y diccionarios para las columnas de baja
cardinalidad: 462,732 filas repiten 16 alcaldías y unos mil giros, así que cada
valor se guarda una vez y las filas solo apuntan a él.

Uso:
    python convertir_denue_a_parquet.py denue_inegi_09_.csv data/

Genera en el directorio de salida:
    denue_cdmx.parquet          462,732 filas  ~28 MB  (todo el DENUE, sin pérdida)
    denue_construccion.parquet   20,957 filas  ~1.8 MB (solo los giros de construcción)

Requiere `pandas` y `pyarrow`, que no están en los requirements del repositorio:
esto es una herramienta de preparación de datos, se corre a mano cuando INEGI
publica una actualización, y su salida es lo único que se versiona.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Claves catastrales y administrativas que empiezan por cero. Leer el CSV sin
# `dtype=str` las convertiría en enteros y "09" (Ciudad de México) pasaría a ser
# 9, rompiendo cualquier cruce posterior con otras fuentes del INEGI.
COLUMNAS_CATEGORICAS = (
    "nombre_act", "codigo_act", "municipio", "entidad", "cve_ent", "cve_mun",
    "per_ocu", "tipoUniEco", "tipo_vial", "tipo_asent", "localidad", "fecha_alta",
)

# Los 99 giros del DENUE que forman la cadena de valor de la construcción, tal
# como los curó el modelo geoespacial: obra y trabajos especializados, extracción
# de materia prima, manufactura de materiales, comercio mayorista y minorista,
# logística y arrendamiento, y servicios profesionales.
GIROS_CONSTRUCCION = (
    "Edificación de inmuebles comerciales y de servicios, excepto la supervisión",
    "Edificación de vivienda unifamiliar",
    "Edificación de naves y plantas industriales, excepto la supervisión",
    "Edificación de vivienda multifamiliar",
    "Construcción de obras de generación y conducción de energía eléctrica",
    "Construcción de obras de urbanización",
    "Construcción de carreteras, puentes y similares",
    "Construcción de sistemas de distribución de petróleo y gas",
    "Construcción de obras para transporte eléctrico y ferroviario",
    "Construcción de obras marítimas, fluviales y subacuáticas",
    "Construcción de obras para telecomunicaciones",
    "Construcción de obras para el tratamiento, distribución y suministro de agua y drenaje",
    "Construcción de plantas de refinería y petroquímica",
    "Otras construcciones de ingeniería civil",
    "Construcción de presas y represas",
    "Preparación de terrenos para la construcción",
    "División de terrenos",
    "Trabajos de cimentaciones",
    "Montaje de estructuras de concreto prefabricadas",
    "Montaje de estructuras de acero prefabricadas",
    "Instalaciones eléctricas en construcciones",
    "Instalaciones de sistemas centrales de aire acondicionado y calefacción",
    "Instalaciones hidrosanitarias y de gas",
    "Otras instalaciones y equipamiento en construcciones",
    "Instalación de señalamientos y protecciones en obras viales",
    "Trabajos de albañilería",
    "Trabajos de pintura y otros cubrimientos de paredes",
    "Colocación de pisos flexibles y de madera",
    "Colocación de pisos cerámicos y azulejos",
    "Colocación de muros falsos y aislamiento",
    "Trabajos de enyesado, empastado y tiroleado",
    "Realización de trabajos de carpintería en el lugar de la construcción",
    "Otros trabajos de acabados en edificaciones",
    "Otros trabajos especializados para la construcción",
    "Otros trabajos en exteriores",
    "Supervisión de edificación de inmuebles comerciales y de servicios",
    "Supervisión de construcción de otras obras de ingeniería civil",
    "Supervisión de construcción de vías de comunicación",
    "Supervisión de edificación residencial",
    "Supervisión de edificación de naves y plantas industriales",
    "Supervisión de construcción de obras para el tratamiento, distribución y suministro de agua, drenaje y riego",
    "Supervisión de división de terrenos y de construcción de obras de urbanización",
    "Supervisión de construcción de obras para petróleo y gas",
    "Supervisión de construcción de obras de generación y conducción de energía eléctrica y de obras para telecomunicaciones",
    "Minería de arena y grava para la construcción",
    "Minería de piedra caliza",
    "Minería de piedra de yeso",
    "Fabricación de productos de madera para la construcción",
    "Tratamiento de la madera y fabricación de postes y durmientes",
    "Fabricación de tubos y bloques de cemento y concreto",
    "Fabricación de concreto",
    "Fabricación de productos preesforzados de concreto",
    "Fabricación de otros productos de cemento y concreto",
    "Fabricación de cemento y productos a base de cemento en plantas integradas",
    "Fabricación de productos de asfalto",
    "Fabricación de azulejos y losetas no refractarias",
    "Fabricación de yeso y productos de yeso",
    "Fabricación de productos a base de piedras de cantera",
    "Fabricación de ladrillos no refractarios",
    "Fabricación de vidrio",
    "Fabricación de fibra de vidrio",
    "Fabricación de espumas y productos de poliestireno",
    "Fabricación de tubería y conexiones, y tubos para embalaje",
    "Fabricación de pinturas y recubrimientos",
    "Fabricación de estructuras metálicas",
    "Fabricación de productos de herrería",
    "Fabricación de tubos y postes de hierro y acero",
    "Fabricación de herrajes y cerraduras",
    "Fabricación de cables de conducción eléctrica",
    "Fabricación de enchufes, contactos, fusibles y otros accesorios para instalaciones eléctricas",
    "Fabricación de equipo de aire acondicionado y calefacción",
    "Fabricación de muebles de baño",
    "Fabricación de cocinas integrales y muebles modulares de baño",
    "Fabricación de maquinaria y equipo para la construcción",
    "Comercio al por mayor de cemento, tabique y grava",
    "Comercio al por mayor de materiales metálicos para la construcción y la manufactura",
    "Comercio al por mayor de madera para la construcción y la industria",
    "Comercio al por mayor de otros materiales para la construcción, excepto de madera y metálicos",
    "Comercio al por mayor de pintura",
    "Comercio al por mayor de vidrios y espejos",
    "Comercio al por mayor de maquinaria y equipo para la construcción y la minería",
    "Comercio al por menor en ferreterías y tlapalerías",
    "Comercio al por menor de materiales para la construcción en tiendas de autoservicio especializadas",
    "Comercio al por menor de pisos y recubrimientos cerámicos",
    "Comercio al por menor de vidrios y espejos",
    "Comercio al por menor de pintura",
    "Autotransporte local de materiales para la construcción",
    "Autotransporte foráneo de materiales para la construcción",
    "Alquiler de maquinaria y equipo para construcción, minería y actividades forestales",
    "Inmobiliarias y corredores de bienes raíces",
    "Servicios de administración de bienes raíces",
    "Otros servicios relacionados con los servicios inmobiliarios",
    "Servicios de arquitectura",
    "Servicios de arquitectura de paisaje y urbanismo",
    "Servicios de ingeniería",
    "Servicios de inspección de edificios",
    "Diseño y decoración de interiores",
    "Servicios de dibujo",
    "Servicios de instalación y mantenimiento de áreas verdes",
)


def convertir(entrada: Path, salida: Path) -> None:
    """Lee el CSV del INEGI y escribe los dos Parquet en `salida`."""
    salida.mkdir(parents=True, exist_ok=True)

    # `dtype=str` preserva los ceros a la izquierda; el encoding es el que usa
    # el INEGI en sus descargas, no UTF-8.
    df = pd.read_csv(entrada, encoding="latin-1", low_memory=False, dtype=str)
    print(f"Leídas {len(df):,} filas × {len(df.columns)} columnas de {entrada.name}")

    # Las coordenadas sí son numéricas: son el eje de todo el análisis espacial.
    for columna in ("latitud", "longitud"):
        df[columna] = pd.to_numeric(df[columna], errors="coerce")

    for columna in COLUMNAS_CATEGORICAS:
        if columna in df.columns:
            df[columna] = df[columna].astype("category")

    ruta_completo = salida / "denue_cdmx.parquet"
    df.to_parquet(ruta_completo, compression="zstd", index=False)

    construccion = df[df["nombre_act"].astype(str).isin(GIROS_CONSTRUCCION)].copy()
    # Un subconjunto hereda el diccionario completo del padre: sin podarlo, el
    # archivo de 21 mil filas seguiría cargando las mil categorías de las que
    # solo usa 99.
    for columna in construccion.columns:
        if str(construccion[columna].dtype) == "category":
            construccion[columna] = construccion[columna].cat.remove_unused_categories()

    ruta_construccion = salida / "denue_construccion.parquet"
    construccion.to_parquet(ruta_construccion, compression="zstd", index=False)

    for ruta, filas in ((ruta_completo, len(df)), (ruta_construccion, len(construccion))):
        print(f"  {ruta.name:28} {filas:>7,} filas  {ruta.stat().st_size / 1e6:6.2f} MB")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("entrada", type=Path, help="CSV del DENUE tal como lo publica INEGI")
    parser.add_argument("salida", type=Path, nargs="?", default=Path("data"),
                        help="directorio donde escribir los Parquet (por defecto: data/)")
    args = parser.parse_args()

    if not args.entrada.is_file():
        print(f"No existe el archivo de entrada: {args.entrada}", file=sys.stderr)
        return 1

    convertir(args.entrada, args.salida)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

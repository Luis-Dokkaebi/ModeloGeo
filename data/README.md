# `data/` — El DENUE de la Ciudad de México

Directorio Estadístico Nacional de Unidades Económicas (**DENUE**) del INEGI,
entidad 09 (Ciudad de México). Es el insumo del modelo de prospección de este
repositorio.

| Archivo | Contenido | Filas | Peso |
| --- | --- | ---: | ---: |
| `denue_cdmx.parquet` | El DENUE completo de CDMX, sin filtrar | 462,732 | 27.9 MB |
| `denue_construccion.parquet` | Solo los 99 giros de la cadena de valor de la construcción | 20,957 | 1.7 MB |
| `denue.sqlite` | El catálogo que consume la plataforma: 14 de las 42 columnas, 3 índices | 20,957 | 5.6 MB |

Los dos Parquet tienen las mismas 42 columnas que publica el INEGI, con los
mismos nombres y en el mismo orden. El SQLite no: ver más abajo.

## Por qué Parquet y no el CSV original

El CSV que publica el INEGI pesa **248 MB**. GitHub avisa a partir de 50 MB y
rechaza a los 100 MB, así que versionarlo no era una opción. En Parquet baja a
28 MB **sin perder una sola celda** y entra sin Git LFS.

La reducción del 89% no viene de tirar datos sino de dos propiedades del
formato: almacenamiento por columna —que comprime bien porque los valores de una
misma columna se parecen entre sí— y diccionarios para las columnas de baja
cardinalidad. 462,732 filas repiten 16 alcaldías y unos mil giros: cada valor se
guarda una vez y las filas apuntan a él.

## Conversión sin pérdida — cómo se verificó

No se da por supuesto: se comparó el Parquet contra el CSV celda por celda.

- Misma forma (462,732 × 42), mismas columnas en el mismo orden.
- Las 40 columnas de texto: **idénticas, cero celdas distintas**.
- Coordenadas: diferencia máxima `0.0000000000` en latitud y longitud.
- **Ceros a la izquierda preservados**: `cve_ent` sigue siendo `"09"` y no `9`,
  `cve_mun` sigue siendo `"017"`, `cod_postal` sigue siendo `"15700"`. Este es
  el error clásico al convertir datos del INEGI, y no es cosmético: si `"09"` se
  vuelve `9`, cualquier cruce posterior con otra fuente del instituto falla en
  silencio.
- Acentos correctos al pasar de latin-1 (el encoding del INEGI) a UTF-8.

`tests/test_convertir_denue_a_parquet.py` fija esas garantías como pruebas.

## `denue.sqlite` — el catálogo de la plataforma

Es el artefacto que consume [`HOLTMONT-PYTHON`](https://github.com/Luis-Dokkaebi/HOLTMONT-PYTHON)
(se copia a `api/data/denue.sqlite`). No sustituye al Parquet: lo complementa.

**Por qué existe.** Leer el Parquet desde la API exige `pandas` + `pyarrow` +
`numpy`, que descomprimidos suman **251 MB** y no caben en el límite de 250 MB de
una función serverless de Vercel. El SQLite se consulta con el módulo `sqlite3`
de la **biblioteca estándar**: la plataforma no gana ni una dependencia. `pandas`
y `pyarrow` se quedan de este lado, en el ETL.

**Qué lleva.** 14 de las 42 columnas —las que pinta el mapa y las que forman la
ficha del negocio— y tres índices, uno por cada acceso que hace la plataforma:

```sql
CREATE TABLE denue (
  id TEXT PRIMARY KEY, nom_estab TEXT, nombre_act TEXT, codigo_act TEXT,
  per_ocu TEXT, telefono TEXT, correoelec TEXT, www TEXT,
  municipio TEXT, nom_vial TEXT, numero_ext TEXT, cod_postal TEXT,
  latitud REAL, longitud REAL
);
CREATE INDEX idx_mun ON denue(municipio);   -- filtro por alcaldía
CREATE INDEX idx_act ON denue(codigo_act);  -- filtro por giro
CREATE INDEX idx_geo ON denue(latitud, longitud);  -- bbox del mapa
```

`cod_postal` es **TEXT**, igual que en el Parquet y por el mismo motivo: `"01000"`
guardado como entero se vuelve `1000` y deja de cruzar. `cve_ent` y `cve_mun` no
viajan al SQLite —no están entre las 14 columnas— pero siguen intactas en los
Parquet, que son la fuente.

**Es determinista.** Dos construcciones del mismo Parquet producen el mismo
archivo byte por byte. Por eso se puede versionar sin que cada regeneración meta
5 MB de ruido en el diff. El script borra la salida antes de escribir: SQLite
reusa las páginas libres del archivo que encuentra, y construir encima de una
corrida anterior daría bytes distintos con el mismo contenido.

`tests/test_construir_sqlite.py` fija todo lo anterior como pruebas, incluidas
las cifras: 20,957 filas, 1,972 proveedores de 11+ personas con contacto, y el
techo de peso.

## Cómo regenerarlos

Cuando el INEGI publique una actualización, los dos pasos en orden:

```bash
# 1. CSV del INEGI (248 MB) → los dos Parquet
python scripts/convertir_denue_a_parquet.py denue_inegi_09_.csv data/

# 2. Parquet de construcción → el SQLite de la plataforma
python scripts/construir_sqlite.py
#    equivale a:
#    python scripts/construir_sqlite.py data/denue_construccion.parquet data/denue.sqlite
```

Ambos pasos requieren `pandas` y `pyarrow` (`pip install -r requirements.txt`).

El CSV crudo **no se versiona**: pesa mil veces más que su parte útil y se
vuelve a descargar del INEGI cuando hace falta.

## Nota para el notebook

`modelo_analisis_geoespacial (2).ipynb` todavía lee el CSV desde
`/content/drive/MyDrive/ST_B2C/denue_inegi_09_.csv`. Con estos archivos en el
repositorio esa lectura puede sustituirse por:

```python
df = pd.read_parquet("data/denue_construccion.parquet")
```

que además evita el paso de filtrar los giros en el notebook — el archivo ya
viene filtrado. **Este cambio no se hizo aquí**: tocar el notebook es un cambio
aparte del de versionar el dato.

## Sobre el contenido

Son datos **públicos y abiertos** del INEGI: nombre del establecimiento, giro,
domicilio, tamaño, coordenadas y —cuando la unidad económica los declaró—
teléfono, correo y sitio web. No hay datos personales de particulares ni
información interna de la empresa.

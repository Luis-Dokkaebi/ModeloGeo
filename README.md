# ModeloGeo — Prospección geoespacial sobre el DENUE

Modelo de prospección de proveedores y clientes del sector construcción en la
Ciudad de México, construido sobre el DENUE del INEGI.

## Qué hay aquí

| Ruta | Qué es |
| --- | --- |
| `modelo_analisis_geoespacial (2).ipynb` | El modelo: mapa con filtros por alcaldía y giro, selección por polígono, y un agente que analiza el sitio web del negocio o redacta la solicitud de cotización |
| `data/` | El DENUE de CDMX en Parquet — 462,732 establecimientos, y el subconjunto de 20,957 de construcción — más `denue.sqlite`, el catálogo que consume la plataforma. Ver [`data/README.md`](data/README.md) |
| `scripts/convertir_denue_a_parquet.py` | Regenera los Parquet de `data/` a partir del CSV que publica el INEGI |
| `scripts/construir_sqlite.py` | Construye `data/denue.sqlite` desde el Parquet de construcción: 14 columnas, 3 índices, 5.6 MB |
| `scripts/enriquecer_sitios.py` | Recorre los 2,848 sitios web del DENUE fuera de línea y guarda el texto en `data/cache_sitios.jsonl` |
| `tests/` | Pruebas del ETL |

## El universo de datos

De los 462,732 establecimientos de la Ciudad de México, **20,957 (4.5%)** caen
en los 99 giros de la cadena de valor de la construcción: obra y trabajos
especializados, extracción de materia prima, manufactura de materiales,
comercio mayorista y minorista, logística y arrendamiento, y servicios
profesionales.

De esos 20,957:

| | Registros | % |
| --- | ---: | ---: |
| Con teléfono | 8,170 | 39.0% |
| Con correo | 7,324 | 34.9% |
| Con sitio web | 2,848 | 13.6% |
| **Con algún dato de contacto** | **13,072** | **62.4%** |

El dato del sitio web decide la forma del agente: solo el 13.6% de los negocios
tiene página, así que la rama de scraping se activa en uno de cada siete casos y
las otras seis veces el grafo va directo a redactar el contacto.

Filtrando a empresas de **11 o más personas con al menos un dato de contacto**
quedan **1,972 proveedores** — una lista que un equipo de compras puede recorrer
de verdad.

## El catálogo para la plataforma

`data/denue.sqlite` es lo que consume [`HOLTMONT-PYTHON`](https://github.com/Luis-Dokkaebi/HOLTMONT-PYTHON)
como módulo de prospección. Se consulta con el `sqlite3` de la biblioteca
estándar, así que la plataforma no gana ninguna dependencia: `pandas` y
`pyarrow` se quedan en el ETL de este repositorio, que es el punto de la
arquitectura. Se regenera con:

```bash
python scripts/construir_sqlite.py
```

El detalle —esquema, índices, por qué no es el Parquet y por qué es
determinista— está en [`data/README.md`](data/README.md).

## El enriquecimiento de sitios

De los 20,957 establecimientos, **2,848 (13.6%)** declararon sitio web. El agente
de la plataforma necesita el texto de esas páginas, y pedirlo en vivo significa
el vendedor esperando de 5 a 30 segundos a que responda un servidor ajeno.

Como el conjunto es finito y conocido, se recorre aquí una vez:

```bash
python scripts/enriquecer_sitios.py --limite 100   # una tanda
python scripts/enriquecer_sitios.py                # los que falten
```

Genera `data/cache_sitios.jsonl`, una línea por establecimiento. Tres propiedades
que lo hacen usable de verdad:

- **Reanudable.** Se escribe línea a línea; un corte a mitad conserva lo hecho y
  la siguiente corrida sigue donde iba.
- **Los fallos se guardan, con fecha.** 1 de cada 4 sitios del DENUE falló al
  medirlo. Sin anotar el fallo, cada corrida se va en esperar los timeouts de los
  mismos sitios muertos. Reintentarlos es una decisión explícita
  (`--reintentar-fallos`).
- **No se muere por un sitio.** Un certificado vencido en el número 300 no tira
  las 2,548 lecturas que faltan.

Este job vive aquí y no en la plataforma por la misma razón que el ETL: en este
repositorio sí caben las herramientas pesadas, y en una función serverless de
250 MB no.

## Cómo correr las pruebas

```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```

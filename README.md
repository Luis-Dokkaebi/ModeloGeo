# ModeloGeo — Prospección geoespacial sobre el DENUE

Modelo de prospección de proveedores y clientes del sector construcción en la
Ciudad de México, construido sobre el DENUE del INEGI.

## Qué hay aquí

| Ruta | Qué es |
| --- | --- |
| `modelo_analisis_geoespacial (2).ipynb` | El modelo: mapa con filtros por alcaldía y giro, selección por polígono, y un agente que analiza el sitio web del negocio o redacta la solicitud de cotización |
| `data/` | El DENUE de CDMX en Parquet — 462,732 establecimientos, y el subconjunto de 20,957 de construcción. Ver [`data/README.md`](data/README.md) |
| `scripts/convertir_denue_a_parquet.py` | Regenera los archivos de `data/` a partir del CSV que publica el INEGI |
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

## Cómo correr las pruebas

```bash
pip install -r requirements.txt
python -m pytest tests/ -v
```

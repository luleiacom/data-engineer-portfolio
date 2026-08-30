# Pipeline de Datos con Arquitectura Medallion — Austin Bikeshare

**Proyecto de portfolio** — Data Engineering 

## Objetivo

Diseñar e implementar un pipeline de datos que transforme datos crudos en activos limpios, confiables y listos para negocio, siguiendo la **Arquitectura Medallion (Bronze → Silver → Gold)** — el mismo patrón de diseño que se pide explícitamente en el puesto que motivó este proyecto.

El caso de uso elegido es el dataset público de viajes de bicicletas compartidas de la ciudad de Austin, Texas, que ofrece volumen real (más de 2 millones de registros) y una estructura similar a los datos operacionales que se procesan en un entorno de producción.

## Arquitectura

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   BRONZE    │ ───▶ │   SILVER    │ ───▶ │    GOLD     │
│ Datos crudos│      │ Limpios,    │      │ Agregados,  │
│ sin procesar│      │ tipados y   │      │ listos para │
│             │      │ particionados│      │ negocio     │
└─────────────┘      └─────────────┘      └─────────────┘
```

- **Bronze**: datos descargados directamente de la fuente oficial (data.austintexas.gov), sin ninguna transformación, preservando el dato original tal como llega.
- **Silver**: limpieza de tipos (parseo de fechas/horas), eliminación de registros inválidos (duración nula o negativa, estación faltante) y **particionado físico por fecha** para optimizar el acceso.
- **Gold**: agregación de negocio — total de viajes y duración promedio por estación y por mes, particionado por mes, listo para consumo directo desde un dashboard de BI.

## Stack utilizado

- **PySpark** — procesamiento distribuido de datos
- **Delta Lake** — formato de tabla transaccional con soporte nativo de particionado
- **Databricks Free Edition** — plataforma de ejecución (notebooks + compute serverless)

*Nota: la arquitectura fue diseñada originalmente sobre Google Cloud Platform (BigQuery, Cloud Storage, Dataform). La ejecución se migró a Databricks por una limitación de la cuenta gratuita de GCP, documentada en detalle en la siguiente sección.*

## Problema técnico encontrado y resolución

Durante la implementación en BigQuery (usando el modo Sandbox, sin cuenta de facturación activa), las tablas creadas con `CREATE TABLE ... PARTITION BY ... AS SELECT` se creaban exitosamente —el job reportaba éxito y procesaba los bytes esperados— pero quedaban con **0 filas**, sin ningún mensaje de error visible.

**Proceso de diagnóstico:**
1. Se descartó un problema de caché de la interfaz web, verificando el resultado directamente por línea de comandos (`bq` CLI) y por metadatos de la tabla (`bq show --format=prettyjson`), confirmando `"numRows": "0"` de forma consistente.
2. Se aisló la causa probando cada componente de la query por separado (filtro `WHERE`, `PARTITION BY`, `CLUSTER BY`), confirmando que una copia simple sin partición funcionaba correctamente con las ~385.000 filas esperadas, pero cualquier versión con `PARTITION BY` resultaba en 0 filas.
3. Se confirmó la causa raíz al intentar una sentencia `INSERT` (DML) explícita, que devolvió el error: *"DML queries are not allowed in the free tier. Set up a billing account to remove this restriction."*
4. Conclusión: en cuentas Sandbox sin facturación habilitada, las tablas particionadas creadas vía CTAS fallan silenciosamente en la escritura de datos, ya que el motor requiere una operación de tipo DML internamente para poblarlas.

**Resolución:** ante la imposibilidad de activar el Free Trial de GCP (requiere tarjeta de crédito/débito, no disponible), se migró la demostración de particionado a Databricks Free Edition, que no tiene esta restricción. El pipeline se reconstruyó en PySpark/Delta Lake, manteniendo el mismo diseño conceptual (Bronze/Silver/Gold) y el mismo objetivo de demostrar particionado funcional.

## Evidencia de particionado funcional

La tabla Silver se guardó particionada físicamente por fecha:

```
partitionColumns: ["fecha"]
numFiles: 3851
```

Al ejecutar una consulta filtrando por fecha, el plan de ejecución confirma el uso de **partition pruning** (lectura selectiva de archivos, sin escanear la tabla completa):

```
PartitionFilters: [isnotnull(fecha#...), (fecha#... = 2023-10-31)]
```

Esto demuestra en la práctica el mismo principio que se buscaba validar en BigQuery: reducir el volumen de datos leídos al filtrar por la columna de partición, con el consiguiente ahorro de costo y tiempo de consulta.

## Resultados por capa

| Capa | Descripción | Filas |
|---|---|---|
| Bronze | Datos crudos sin procesar | 2.271.153 |
| Silver | Limpios, tipados, particionados por fecha | 2.271.153 |
| Gold | Agregado por estación y mes, particionado por mes | 8.262 |

## Código de referencia (Silver)

```python
from pyspark.sql.functions import to_timestamp, col, to_date

df_silver = (
    df_bronze
    .withColumn("checkout_ts", to_timestamp(col("Checkout Datetime"), "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("trip_duration", col("Trip Duration Minutes").cast("double"))
    .filter(col("checkout_ts").isNotNull())
    .filter(col("Checkout Kiosk").isNotNull())
    .filter(col("trip_duration") > 0)
    .withColumn("fecha", to_date(col("checkout_ts")))
)

df_silver.write.mode("overwrite").partitionBy("fecha").format("delta").saveAsTable("silver_bikeshare_trips")
```

## Conclusión

Este proyecto demuestra, de punta a punta, la capacidad de diseñar e implementar una arquitectura Medallion completa, incluyendo limpieza de datos, particionado para optimización de consultas, y agregación para consumo de negocio — además de la capacidad de diagnosticar y resolver un problema técnico real de infraestructura bajo restricciones de entorno (cuenta gratuita sin facturación), adaptando la solución sin perder el objetivo original.

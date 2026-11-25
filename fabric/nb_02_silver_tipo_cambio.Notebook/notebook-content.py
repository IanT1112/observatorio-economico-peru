# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "70f30812-eefd-4cd4-8fb9-2bf389149ec1",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "e4605945-f370-426b-b894-1c71c815e95b",
# META       "known_lakehouses": [
# META         {
# META           "id": "70f30812-eefd-4cd4-8fb9-2bf389149ec1"
# META         },
# META         {
# META           "id": "09a5682f-1f0d-4c70-864e-d52c019d6425"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

MESES = {"Ene": 1, "Feb": 2, "Mar": 3, "Abr": 4, "May": 5, "Jun": 6,
         "Jul": 7, "Ago": 8, "Sep": 9, "Set": 9, "Oct": 10, "Nov": 11, "Dic": 12}
mapa_meses = F.create_map(*[x for k, v in MESES.items() for x in (F.lit(k), F.lit(v))])

bronze = spark.read.table("lh_bronze.dbo.bcrp_tipo_cambio")
print("Filas en bronze:", bronze.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

partes = F.split("periodo_raw", r"\.")   # "03.Ene.22" -> ["03", "Ene", "22"]

silver = (
    bronze
    .filter((F.col("tc_compra") != "n.d.") & (F.col("tc_venta") != "n.d."))
    .withColumn("fecha", F.make_date(
        F.lit(2000) + partes.getItem(2).cast("int"),
        F.element_at(mapa_meses, partes.getItem(1)),
        partes.getItem(0).cast("int"),
    ))
    .withColumn("tc_compra",   F.col("tc_compra").cast("decimal(10,4)"))
    .withColumn("tc_venta",    F.col("tc_venta").cast("decimal(10,4)"))
    .withColumn("tc_promedio", ((F.col("tc_compra") + F.col("tc_venta")) / 2).cast("decimal(10,4)"))
    # deduplicar: si una fecha llega dos veces, gana la ingesta más reciente
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("fecha").orderBy(F.col("_ingested_at").desc())))
    .filter("_rn = 1")
    .select("fecha", "tc_compra", "tc_venta", "tc_promedio", "_ingested_at")
    .withColumn("_processed_at", F.current_timestamp())
)

print("Filas en silver:", silver.count())
display(silver.orderBy("fecha").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Marcar (sin eliminar) los días donde venta < compra
silver = silver.withColumn("flag_spread_invertido", F.col("tc_venta") < F.col("tc_compra"))

total        = silver.count()
esperadas    = bronze.filter((F.col("tc_compra") != "n.d.") & (F.col("tc_venta") != "n.d.")).count()
n_invertidos = silver.filter("flag_spread_invertido").count()

# Si falla alguno de estos, NO se guarda nada
bloqueantes = {
    "sin_fechas_nulas":      silver.filter(F.col("fecha").isNull()).count() == 0,
    "fechas_unicas":         silver.select("fecha").distinct().count() == total,
    "tc_en_rango_2.5_a_5.0": silver.filter(~F.col("tc_compra").between(2.5, 5.0)
                                         | ~F.col("tc_venta").between(2.5, 5.0)).count() == 0,
    "sin_perdida_de_filas":  total == esperadas,
    "spread_invertido_<1%":  n_invertidos / total < 0.01,
}

for nombre, ok in bloqueantes.items():
    print("✅" if ok else "❌", nombre)

if n_invertidos:
    print(f"⚠️  advertencia: {n_invertidos} día(s) con venta < compra (marcados en flag_spread_invertido)")

fallidos = [k for k, ok in bloqueantes.items() if not ok]
if fallidos:
    raise ValueError(f"Calidad de datos falló: {fallidos}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# > **Hallazgo de calidad:** 1 día (2022-06-23) con venta < compra (dif. 0,0038). Se conserva y se marca con flag_spread_invertido.

# CELL ********************

(silver.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_silver.dbo.tipo_cambio_diario"))

spark.sql("""
    SELECT MIN(fecha) AS desde, MAX(fecha) AS hasta, COUNT(*) AS filas,
           SUM(CAST(flag_spread_invertido AS INT)) AS dias_flag,
           ROUND(AVG(tc_promedio), 4) AS tc_prom_periodo
    FROM lh_silver.dbo.tipo_cambio_diario
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

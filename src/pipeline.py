from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, round, to_date, col
import shutil, os

def main():
    spark = SparkSession.builder.appName("Weather Monthly Summary").getOrCreate()

    # 1. Leer CSV original
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("data/raw/london_weather.csv")

    # 2. Convertir 'date' de entero a fecha
    df = df.withColumn("date", to_date(col("date").cast("string"), "yyyyMMdd"))

    # 3. Extraer año y mes
    df = df.withColumn("year", year("date")).withColumn("month", month("date"))

    # 4. Agrupar y calcular medias
    df_summary = df.groupBy("year", "month").mean()

    # 5. Renombrar columnas limpias
    for old in df_summary.columns:
        if old.startswith("avg(") and not ("year" in old or "month" in old):
            new = old.replace("avg(", "").replace(")", "").replace("mean_", "")
            df_summary = df_summary.withColumnRenamed(old, new)

    # 6. Redondear a 3 decimales
    for c in [c for c in df_summary.columns if c not in ("year", "month")]:
        df_summary = df_summary.withColumn(c, round(col(c), 3))

    # 7. Ordenar por año y mes
    df_summary = df_summary.orderBy("year", "month")

    # 8. Eliminar directorio anterior si existe
    output_dir = "data/processed/monthly_summary_csv"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    # 9. Exportar a CSV en formato europeo
    df_summary.coalesce(1).write \
        .option("header", True) \
        .option("sep", ";") \
        .mode("overwrite") \
        .csv(output_dir)

    spark.stop()

if __name__ == "__main__":
    main()


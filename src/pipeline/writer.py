from pyspark.sql import DataFrame
import pyspark.sql.functions as f

def write_partitioned(df: DataFrame, output_base_path: str):

    # Opcion con particionado de pyspark
    # df_write = df.withColumn(
    #     "fecha_proceso",
    #     f.date_format("fecha_proceso", "yyyyMMdd")
    # )

    # (
    #     df_write.write 
    #     .mode("overwrite") 
    #     .partitionBy("fecha_proceso","pais") 
    #     .parquet(output_base_path)
    # )

    df_write = (
        df.withColumn(
            "fecha_folder",
            f.date_format("fecha_proceso", "yyyyMMdd")
        )
    )

    df_write = df_write.cache()

    pairs = [
        (row.fecha_folder, row.pais)
        for row in (
            df_write
            .select("fecha_folder", "pais")
            .distinct()
            .collect()
        )
    ]

    for fecha_str, pais in pairs:
        (
            df_write
            .filter(
                (f.col("fecha_folder") == fecha_str) &
                (f.col("pais") == pais)
            )
            .write
            .mode("overwrite")
            .parquet(f"{output_base_path}/{fecha_str}/{pais}")
        )
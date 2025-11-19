from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def get_schema():
    return StructType([
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("transporte", IntegerType(), True),
        StructField("ruta", IntegerType(), True),   
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", DecimalType(), True),
        StructField("cantidad", IntegerType(), True),
        StructField("unidad", IntegerType(), True)
    ])

def read_input(spark: SparkSession, input_path: str):
    schema = get_schema()
    df = (
        spark.read
            .option("header", True)
            .schema(schema)
            .csv(input_path)
    )

    # parsear fecha_proceso a DateType
    df = df.withColumn("fecha_proceso", f.to_date("fecha_proceso", "yyyyMMdd"))

    return df

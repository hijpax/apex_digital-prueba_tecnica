from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def get_schema():
    """
    Define and return the schema for the input CSV file.

    Returns:
        StructType: Schema used to read the raw dataset.
    """

    return StructType([
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("transporte", IntegerType(), True),
        StructField("ruta", IntegerType(), True),   
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", DecimalType(10, 2), True),
        StructField("cantidad", IntegerType(), True),
        StructField("unidad", StringType(), True)
    ])

def read_input(spark: SparkSession, input_path: str):
    """
    Read the input CSV using a predefined schema to ensure consistent types.

    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to the raw CSV file.

    Returns:
        DataFrame: Loaded dataset with enforced schema.
    """

    schema = get_schema()
    df = (
        spark.read
            .option("header", True)
            .schema(schema)
            .csv(input_path)
    )

    return df

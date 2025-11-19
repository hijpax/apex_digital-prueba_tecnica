from pyspark.sql import SparkSession


def create_spark(app_name: str = "entregas_rutina", env: str = "develop") -> SparkSession:

    builder = (
        SparkSession.builder
        .appName(f"{app_name}-{env}")
        .master("local[*]")  
    )

    # Configuracion de entorno local
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .config("spark.sql.caseSensitive", "false")
    )

    spark = builder.getOrCreate()

    # Nivel de log
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_spark() -> SparkSession:
    return SparkSession.getActiveSession()

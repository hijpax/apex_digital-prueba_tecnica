# src/pipeline/transformers.py

import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def filter_by_date_and_country(df: DataFrame, cfg) -> DataFrame:

    start = cfg.filters.start_date
    end = cfg.filters.end_date
    country = cfg.filters.country

    df_filtered = (
        df
        .filter(f.col("fecha_proceso").between(start, end))
        .filter(f.col("pais") == country)
    )

    return df_filtered


def standardize_column_names(df: DataFrame) -> DataFrame:
    new_names = {}
    for col in df.columns:
        clean = (
            col.lower()
                .strip()
                .replace(" ", "_")
                .replace("-", "_")
                .replace("/", "_")
        )
        new_names[col] = clean

    for old, new in new_names.items():
        df = df.withColumnRenamed(old, new)

    return df


def normalize_units(df: DataFrame, cfg) -> DataFrame:

    factor = cfg.units.cs_to_st_factor

    df = (
        df.withColumn("cantidad_original", f.col("cantidad"))
        .withColumn("unidad_original", f.col("unidad"))
    )

    df = df.withColumn(
        "cantidad",
        f.when(f.col("unidad") == "CS", f.col("cantidad") * factor)
         .when(f.col("unidad") == "ST", f.col("cantidad"))
         .otherwise(None)
    )

    df = (
        df.withColumn("unidad", f.lit(cfg.units.base_unit))
        # .drop("unidad_original","cantidad_original")
    )

    return df

def add_delivery_type_columns(df: DataFrame, cfg) -> DataFrame:

    routine = cfg.delivery_types.routine      # ["ZPRE", "ZVE1"]
    bonus = cfg.delivery_types.bonus          # ["Z04", "Z05"]

    group_cols = ["pais", "fecha_proceso", "transporte", "ruta", "material", "precio","unidad"]

    df = df.filter(f.col("tipo_entrega").isin(*(routine + bonus)))

    df = (
        df.groupBy(*group_cols)
        .agg(
            f.sum(
                f.when(
                    f.col("tipo_entrega").isin(*routine),
                    f.col("cantidad")
                ).otherwise(0)
            ).alias("unidades_rutina"),
            f.sum(
                f.when(
                    f.col("tipo_entrega").isin(*bonus),
                    f.col("cantidad")
                ).otherwise(0)
            ).alias("unidades_bonificacion"),
            f.sum("cantidad").alias("total_unidades")
        )
    )

    return df

def add_extra_columns(df: DataFrame, cfg) -> DataFrame:

    df = df.withColumn("anio_proceso", f.year("fecha_proceso"))
    df = df.withColumn("mes_proceso", f.month("fecha_proceso"))

    df = df.withColumn("es_bonificada", f.col("unidades_bonificacion") > f.lit(0))

    return df

def apply_business_rules(df: DataFrame, cfg) -> DataFrame:

    df = standardize_column_names(df)

    df = filter_by_date_and_country(df, cfg)

    df = normalize_units(df, cfg)

    df = add_delivery_type_columns(df, cfg)

    df = add_extra_columns(df, cfg)

    return df


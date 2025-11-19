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

    df = df.withColumn("cantidad_original", f.col("cantidad"))

    df = df.withColumn(
        "cantidad_unidades",
        f.when(f.col("unidad") == "CS", f.col("cantidad") * factor)
         .when(f.col("unidad") == "ST", f.col("cantidad"))
         .otherwise(None)
    )

    df = df.withColumn("unidad_base", f.lit(cfg.units.base_unit))

    return df


def apply_business_rules(df: DataFrame, cfg) -> DataFrame:

    df = standardize_column_names(df)

    df = filter_by_date_and_country(df, cfg)

    df = normalize_units(df, cfg)

    return df


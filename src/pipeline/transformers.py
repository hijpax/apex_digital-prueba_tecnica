# src/pipeline/transformers.py

import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def filter_by_date_and_country(df: DataFrame, cfg) -> DataFrame:
    """
    Filter the DataFrame by a date range and optionally by country.

    Args:
        df (DataFrame): Input DataFrame.
        cfg: Configuration object containing start_date, end_date, and country.

    Returns:
        DataFrame: Filtered dataset.
    """

    start = cfg.filters.start_date
    end = cfg.filters.end_date
    country = cfg.filters.country

    df_filtered = df.filter(f.col("fecha_proceso").between(start, end))

    if country and country != "":
        df_filtered = df_filtered.filter(f.col("pais") == country)

    return df_filtered


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names to lowercase snake_case by removing spaces
    and special characters.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Dataset with normalized column names.
    """

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
    """
    Normalize product units by converting CS to ST using a configured factor
    and assigning a single base unit to all records.

    Args:
        df (DataFrame): Input DataFrame.
        cfg: Configuration object with unit conversion settings.

    Returns:
        DataFrame: Dataset with unified units.
    """

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
    """
    Aggregate quantities by delivery type (routine and bonus) and compute
    total units for each group of date, country, route, and material.

    Args:
        df (DataFrame): Input DataFrame already normalized.
        cfg: Configuration object defining routine and bonus delivery types.

    Returns:
        DataFrame: Aggregated dataset with delivery-type unit columns.
    """

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
    """
    Add helper fields such as process year, process month, and a flag
    indicating whether the delivery includes bonus units.

    Args:
        df (DataFrame): Input DataFrame.
        cfg: Configuration object.

    Returns:
        DataFrame: Dataset enriched with additional attributes.
    """

    df = df.withColumn("anio_proceso", f.year("fecha_proceso"))
    df = df.withColumn("mes_proceso", f.month("fecha_proceso"))

    df = df.withColumn("es_bonificada", f.col("unidades_bonificacion") > f.lit(0))

    return df

def rename_final_columns(df: DataFrame) -> DataFrame:
    """
    Rename final output columns to standardized business-friendly names.

    Args:
        df (DataFrame): Transformed DataFrame ready for final adjustments.

    Returns:
        DataFrame: Dataset with standardized column names for the final schema.
    """
    return (
        df.withColumnRenamed("transporte", "transporte_id")
          .withColumnRenamed("ruta", "ruta_id")
          .withColumnRenamed("material", "material_id")
          .withColumnRenamed("precio", "precio_unitario")
          .withColumnRenamed("unidad", "unidad_base")
    )


def apply_business_rules(df: DataFrame, cfg) -> DataFrame:
    """
    Apply the full transformation pipeline: column normalization, filtering,
    unit conversion, delivery-type aggregation, and extra fields.

    Args:
        df (DataFrame): Raw cleaned DataFrame.
        cfg: Configuration object.

    Returns:
        DataFrame: Fully transformed dataset ready for DQ post-checks and writing.
    """

    df = standardize_column_names(df)

    df = filter_by_date_and_country(df, cfg)

    df = normalize_units(df, cfg)

    df = add_delivery_type_columns(df, cfg)

    df = add_extra_columns(df, cfg)

    df = rename_final_columns(df) 

    return df


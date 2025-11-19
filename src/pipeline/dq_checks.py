import pyspark.sql.functions as f
from pyspark.sql import DataFrame

def apply_dq_pre(df: DataFrame, cfg):

    for col_name in cfg.dq.remove_nulls_in:
        df = df.filter(f.col(col_name).isNotNull())
    
    df = df.withColumn("fecha_proceso", f.to_date("fecha_proceso", "yyyyMMdd"))

    df = df.filter(f.col("unidad").isin(*cfg.dq.valid_units))

    df = df.filter(f.col("cantidad") > 0)

    if cfg.dq.drop_duplicates:
        if cfg.dq.dedup_keys:
            df = df.dropDuplicates(cfg.dq.dedup_keys)
        else:
            df = df.dropDuplicates()

    return df


def apply_dq_post(df: DataFrame, cfg):

    # 1) total_unidades > 0
    invalid_total_count = df.filter(f.col("total_unidades") <= 0).count()

    if invalid_total_count > 0:
        raise Exception(
            f"DQ POST FAILED: total_unidades <= 0 en {invalid_total_count} registros."
        )
    else:
        if cfg.app.env == "develop":
            print("DQ POST PASSED: total_unidades <= 0")

    # 2) unidades_rutina >= 0 y unidades_bonificacion >= 0
    invalid_units_count = df.filter(
        (f.col("unidades_rutina") < 0) |
        (f.col("unidades_bonificacion") < 0)
    ).count()

    if invalid_units_count > 0:
        raise Exception(
            f"DQ POST FAILED: unidades_rutina o unidades_bonificacion negativas "
            f"en {invalid_units_count} registros."
        )
    else:
        if cfg.app.env == "develop":
            print("DQ POST PASSED: unidades_rutina o unidades_bonificacion negativas")

    # 3) Consistencia de totales: total_unidades >= unidades_rutina + unidades_bonificacion
    invalid_consistency_count = df.filter(
        f.col("total_unidades") < (f.col("unidades_rutina") + f.col("unidades_bonificacion"))
    ).count()

    if invalid_consistency_count > 0:
        raise Exception(
            "DQ POST FAILED: total_unidades < unidades_rutina + unidades_bonificacion "
            f"en {invalid_consistency_count} registros."
        )
    else:
        if cfg.app.env == "develop":
            print("DQ POST PASSED: total_unidades < unidades_rutina + unidades_bonificacion")
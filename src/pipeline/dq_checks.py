import pyspark.sql.functions as f

def apply_dq_rules(df, cfg):

    for col_name in cfg.dq.remove_nulls_in:
        df = df.filter(f.col(col_name).isNotNull())

    df = df.filter(f.col("unidad").isin(*cfg.dq.valid_units))

    df = df.dropDuplicates(["fecha_proceso", "pais", "material", "tipo_entrega","transporte","ruta","cantidad"])

    return df

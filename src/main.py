
from omegaconf import OmegaConf
from src.utils.spark_utils import create_spark
from src.utils.cli_utils import parse_args
from src.pipeline import reader, transformers

def load_config(env: str, cli_args) -> dict:
    base = OmegaConf.load("config/base.yml")
    env_cfg = OmegaConf.load(f"config/env/{env}.yml")
    config = OmegaConf.merge(base, env_cfg)

    # overrides desde CLI
    if cli_args.start_date:
        config.filters.start_date = cli_args.start_date
    if cli_args.end_date:
        config.filters.end_date = cli_args.end_date
    if cli_args.country:
        config.filters.country = cli_args.country

    return config

def main():
    args = parse_args() 
    config = load_config(args.env,args)

    spark = create_spark(app_name=config.app.name, env=config.app.env)

    df_raw = reader.read_input(spark, config.paths.input)

    df_clean = transformers.apply_business_rules(df_raw, config)

    df_clean.show()


if __name__ == "__main__":
    main()


from omegaconf import OmegaConf
from src.utils.spark_utils import create_spark
from src.utils.cli_utils import parse_args

def load_config(env: str, cli_args) -> dict:
    config = OmegaConf.load("configs/base.yaml")

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
    config = load_config(args.env)
    spark = create_spark(app_name=config.app.name, env=config.app.env)
    return

if __name__ == "__main__":
    main()

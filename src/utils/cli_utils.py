import argparse


def parse_args():
    """
    Parse command-line arguments for environment selection and optional
    execution parameters such as date range and country.

    Returns:
        argparse.Namespace: Parsed CLI arguments.
    """

    parser = argparse.ArgumentParser(
        description="Pipeline de entregas rutine"
    )

    parser.add_argument(
        "--env",
        type=str,
        default="develop",
        choices=["develop", "qa", "prod"],
        help="Entorno de ejecución (default: develop)"
    )

    parser.add_argument(
        "--start_date",
        type=str,
        required=False,
        help="Fecha inicial del rango. Formato: yyyymmdd"
    )

    parser.add_argument(
        "--end_date",
        type=str,
        required=False,
        help="Fecha final del rango. Formato: yyyymmdd"
    )

    parser.add_argument(
        "--country",
        type=str,
        required=False,
        help="Código de país a procesar (ej. SV, GT, HN)"
    )

    return parser.parse_args()

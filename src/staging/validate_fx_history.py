"""Validate a prepared Tiingo Forex staging CSV."""

import argparse
from pathlib import Path

from src.staging.tiingo_fx_history import validate_staging_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Valida campos, preços e duplicações num CSV de staging.",
    )
    parser.add_argument("staging_file", type=Path)
    args = parser.parse_args()

    valid_rows = validate_staging_file(args.staging_file)
    print(
        f"Validação concluída: {valid_rows} registo(s) válido(s), "
        "sem duplicações."
    )


if __name__ == "__main__":
    main()

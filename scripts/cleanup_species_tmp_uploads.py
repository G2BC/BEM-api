"""
Limpa objetos antigos do bucket temporário de fotos de solicitações.

Uso:
    python scripts/cleanup_species_tmp_uploads.py --days 30 --dry-run
    python scripts/cleanup_species_tmp_uploads.py --days 30
"""

import argparse

from app import create_app
from app.services.species_change_request import SpeciesChangeRequestService


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None, help="Retention em dias")
    parser.add_argument("--dry-run", action="store_true", help="Apenas simula")
    return parser.parse_args()


def main():
    args = parse_args()
    app = create_app()
    with app.app_context():
        result = SpeciesChangeRequestService.cleanup_tmp_objects(
            retention_days=args.days,
            dry_run=args.dry_run,
        )
        print(result)


if __name__ == "__main__":
    main()

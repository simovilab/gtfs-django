import csv
from pathlib import Path
from django.core.management.base import BaseCommand
from gtfs.utils.eta_builder import ETABuilder


class Command(BaseCommand):
    help = "Generate stop_times.txt using the ETA module (Deterministic or Bytewax)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--trip",
            type=str,
            default="T100",
            help="Trip ID to generate stop_times for (default: T100)",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for deterministic ETA generation (default: 42)",
        )
        parser.add_argument(
            "--output",
            type=str,
            default="tmp_gtfs/stop_times_eta.txt",
            help="Output file path for generated stop_times (default: tmp_gtfs/stop_times_eta.txt)",
        )

    def handle(self, *args, **options):
        trip_id = options["trip"]
        seed = options["seed"]
        output = Path(options["output"])
        output.parent.mkdir(parents=True, exist_ok=True)

        # Example stops (you can modify these for real feeds)
        stops = [
            ("ST1", "Central Station"),
            ("ST2", "North Park"),
            ("ST3", "University"),
            ("ST4", "Airport"),
        ]

        builder = ETABuilder(seed=seed)
        stop_times = builder.build_stop_times(trip_id, stops)

        # Write to GTFS-like stop_times.txt
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(stop_times[0].keys()))
            writer.writeheader()
            writer.writerows(stop_times)

        self.stdout.write(self.style.SUCCESS(f"stop_times file generated: {output}"))

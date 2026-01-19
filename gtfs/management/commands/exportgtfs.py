from django.core.management.base import BaseCommand, CommandError
from gtfs.utils.schedule import export_gtfs_schedule


class Command(BaseCommand):
    help = "Exports GTFS Schedule data from database to a valid GTFS ZIP archive."

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            type=str,
            default="tmp_gtfs/exported_feed.zip",
            help="Output path for exported GTFS ZIP (default: tmp_gtfs/exported_feed.zip)",
        )

    def handle(self, *args, **options):
        output = options["output"]
        try:
            zip_path = export_gtfs_schedule(output)
            self.stdout.write(
                self.style.SUCCESS(f"Successfully exported GTFS feed to {zip_path}")
            )
        except Exception as e:
            raise CommandError(f"Error exporting GTFS feed: {e}")

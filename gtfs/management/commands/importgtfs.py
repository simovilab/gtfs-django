from django.core.management.base import BaseCommand, CommandError
from gtfs.utils.schedule import import_gtfs


class Command(BaseCommand):
    help = "Imports a GTFS Schedule feed and saves it to the database."

    def add_arguments(self, parser):
        parser.add_argument(
            "feed_path", type=str, help="Path to the GTFS zip file or directory"
        )

    def handle(self, *args, **options):
        feed_path = options["feed_path"]
        try:
            import_gtfs(feed_path)
            self.stdout.write(
                self.style.SUCCESS(f"Successfully imported GTFS feed from {feed_path}")
            )
        except Exception as e:
            raise CommandError(f"Error importing GTFS feed: {e}")

import django
from django.conf import settings

settings.configure(
    INSTALLED_APPS=[
        "django.contrib.contenttypes",
        "django.contrib.auth",
        "gtfs",  # no importa apps_test, no se crearán migraciones
    ],
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
)

django.setup()

from django.test.utils import get_runner

TestRunner = get_runner(settings)
test_runner = TestRunner()
failures = test_runner.run_tests(["tests.test_schedule_crud"])
if failures:
    exit(1)

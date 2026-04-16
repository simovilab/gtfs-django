import django
import pytest


class TestGtfsPackage:
    """Test that the gtfs package is properly configured."""

    def test_django_minimum_version(self):
        """Test that the installed Django meets the minimum requirement (5.2+)."""
        assert django.VERSION >= (5, 2), (
            f"Django {django.get_version()} does not meet the minimum requirement. "
            "gtfs-django requires Django 5.2+."
        )

    def test_gtfs_package_import(self):
        """Test that the gtfs package can be imported."""
        import gtfs

        assert hasattr(gtfs, "__version__")
        assert gtfs.__version__ == "0.1.0"

    def test_gtfs_app_config(self):
        """Test that the gtfs app config has the correct attributes."""
        from gtfs.apps import GtfsConfig

        assert GtfsConfig.name == "gtfs"
        assert GtfsConfig.verbose_name == "GTFS Utilities for Django"
        assert hasattr(GtfsConfig, "default_auto_field")

    def test_abstract_models_can_be_imported(self):
        """Test that all abstract base models can be imported."""
        import gtfs.models as m

        expected = [
            "BaseAgency",
            "BaseStop",
            "BaseRoute",
            "BaseCalendar",
            "BaseCalendarDate",
            "BaseShape",
            "BaseTrip",
            "BaseStopTime",
            "BaseFareAttribute",
            "BaseFareRule",
            "BaseFeedInfo",
            "BaseFeedMessage",
            "BaseTripUpdate",
            "BaseStopTimeUpdate",
            "BaseVehiclePosition",
            "BaseAlert",
        ]
        for name in expected:
            assert hasattr(m, name), f"{name} not found in gtfs.models"

    def test_all_models_are_abstract(self):
        """Test that every model is abstract so no tables are created in consumer projects."""
        import gtfs.models as m

        abstract_models = [
            m.BaseAgency,
            m.BaseStop,
            m.BaseRoute,
            m.BaseCalendar,
            m.BaseCalendarDate,
            m.BaseShape,
            m.BaseTrip,
            m.BaseStopTime,
            m.BaseFareAttribute,
            m.BaseFareRule,
            m.BaseFeedInfo,
            m.BaseFeedMessage,
            m.BaseTripUpdate,
            m.BaseStopTimeUpdate,
            m.BaseVehiclePosition,
            m.BaseAlert,
        ]
        for model in abstract_models:
            assert model._meta.abstract, (
                f"{model.__name__} is not abstract. All models in this package "
                "must be abstract so they can be extended in consumer projects."
            )

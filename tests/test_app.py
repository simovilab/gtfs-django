import django
import pytest


class TestGtfsPackage:
    """Test that the GTFS Django package is properly configured."""

    def test_django_version_supports_composite_keys(self):
        """Test that Django version supports composite primary keys (5.2+)."""
        assert django.VERSION >= (5, 2), (
            f"Django {django.get_version()} does not support composite primary keys. "
            "GTFS models require Django 5.2+ for composite primary key support."
        )

    def test_gtfs_package_import(self):
        """Test that the gtfs package can be imported."""
        import gtfs
        assert hasattr(gtfs, "__version__")
        assert gtfs.__version__ == "0.1.0"

    def test_gtfs_app_config(self):
        """Test that the gtfs Django app config can be imported."""
        from gtfs.apps import GtfsConfig
        # Just test that the class exists and has the right attributes
        assert GtfsConfig.name == "gtfs"
        assert GtfsConfig.verbose_name == "GTFS for Django"
        assert hasattr(GtfsConfig, 'default_auto_field')
        
    def test_models_can_be_imported(self):
        """Test that models can be imported without requiring DB setup."""
        # This will import the models module but not instantiate any models
        import gtfs.models
        assert hasattr(gtfs.models, "Feed")
        assert hasattr(gtfs.models, "Agency")
        assert hasattr(gtfs.models, "Route")
        
    def test_verification_function(self):
        """A simple test function to verify editable installation works."""
        return "Version 1 - Initial test"
        

def simple_test_function():
    """A standalone function to test editable installation."""
    return "Version 1 - Editable installation working!"

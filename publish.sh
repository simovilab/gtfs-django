#!/bin/bash
# GTFS Django Package Publishing Script
# 
# This script contains the commands to publish your package to PyPI.
# Before running, make sure you have:
# 1. Created accounts on PyPI and TestPyPI
# 2. Configured your API tokens
# 3. Tested the package thoroughly

set -e

echo "🏗️  Building GTFS Django package..."

# Clean previous builds
rm -rf dist/ build/ *.egg-info/

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Build the package
echo "📦 Building package..."
python -m build

# Validate the package
echo "✅ Validating package..."
python -m twine check dist/*

echo "✅ Package built successfully!"
echo ""
echo "Next steps:"
echo "1. Test upload to TestPyPI:"
echo "   python -m twine upload --repository testpypi dist/*"
echo ""
echo "2. Test install from TestPyPI:"
echo "   pip install --index-url https://test.pypi.org/simple/ gtfs-django"
echo ""
echo "3. Upload to PyPI:"
echo "   python -m twine upload dist/*"
echo ""
echo "4. Install from PyPI:"
echo "   pip install gtfs-django"
echo ""

echo "📋 Package summary:"
ls -la dist/
echo ""
echo "✨ Your GTFS Django package is ready for publishing!"
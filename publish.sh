#!/bin/bash
# GTFS Django Package Publishing Script
#
# Before running, make sure you have:
# 1. Created accounts on PyPI and TestPyPI
# 2. Configured your API tokens (UV_PUBLISH_TOKEN or keyring)
# 3. Tested the package thoroughly

set -e

# Clean previous builds
rm -rf dist/

# Run tests
echo "Running tests..."
uv run pytest tests/ -v

# Build the package
echo "Building package..."
uv build

echo "Package built successfully!"
echo ""
echo "Next steps:"
echo "1. Test upload to TestPyPI:"
echo "   uv publish --publish-url https://test.pypi.org/legacy/"
echo ""
echo "2. Test install from TestPyPI:"
echo "   pip install --index-url https://test.pypi.org/simple/ gtfs-django"
echo ""
echo "3. Upload to PyPI:"
echo "   uv publish"
echo ""

echo "Package summary:"
ls -la dist/

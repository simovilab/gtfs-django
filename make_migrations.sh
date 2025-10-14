#!/bin/bash

# make_migrations.sh - Generate Django migrations for gtfs-django app
# Usage: ./make_migrations.sh
# 
# This script will always create a fresh 0001_initial.py migration,
# removing any existing migration files first.

set -e  # Exit on any error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${GREEN}🚀 Generating fresh Django migration for gtfs app...${NC}"

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ] || [ ! -d "gtfs" ]; then
    echo -e "${RED}❌ Error: This script must be run from the gtfs-django project root directory${NC}"
    echo "Expected files: pyproject.toml, gtfs/ directory"
    exit 1
fi

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo -e "${RED}❌ Error: Python is not available in PATH${NC}"
    exit 1
fi

# Check if Django is available
if ! python -c "import django" &> /dev/null; then
    echo -e "${YELLOW}⚠️  Django not found. Installing dependencies...${NC}"
    
    # Try to install with uv first, fallback to pip
    if command -v uv &> /dev/null; then
        echo "Installing dependencies with uv..."
        uv sync
    else
        echo "Installing dependencies with pip..."
        pip install -e .
    fi
fi

# Remove existing migration files (except __init__.py)
echo -e "${YELLOW}🗑️  Removing existing migration files...${NC}"
if ls gtfs/migrations/[0-9]*.py &> /dev/null; then
    rm -f gtfs/migrations/[0-9]*.py
    echo -e "${GREEN}✅ Removed old migration files${NC}"
else
    echo -e "${BLUE}ℹ️  No existing migration files to remove${NC}"
fi

# Set environment variables for GIS support
export USE_GIS=1

# Generate fresh initial migration
echo -e "${GREEN}🔧 Running makemigrations to create 0001_initial.py...${NC}"

python -m django makemigrations gtfs --name initial \
    --settings=tests.settings \
    --verbosity=2

# Check if migrations were created
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Fresh 0001_initial.py migration generated successfully!${NC}"
    
    # List the migration file
    echo -e "${GREEN}📁 Created migration file:${NC}"
    if [ -f "gtfs/migrations/0001_initial.py" ]; then
        ls -la gtfs/migrations/0001_initial.py
        echo -e "${BLUE}📋 File size: $(wc -l < gtfs/migrations/0001_initial.py) lines${NC}"
    fi
    
    # Ask if user wants to add to git
    echo ""
    read -p "📝 Add new migration file to git? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git add gtfs/migrations/
        echo -e "${GREEN}✅ Migration file added to git${NC}"
        
        # Show git status
        echo -e "${GREEN}📊 Git status:${NC}"
        git status --porcelain gtfs/migrations/
    fi
    
    echo ""
    echo -e "${GREEN}🎉 Done! Fresh 0001_initial.py is ready for use in other Django projects.${NC}"
    echo -e "${BLUE}📄 Remember to rebuild your package with: uv build${NC}"
else
    echo -e "${RED}❌ Failed to generate migrations${NC}"
    exit 1
fi

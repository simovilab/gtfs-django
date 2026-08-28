# GTFS-RT Tools

Utilities for exploring **GTFS-Realtime** feeds such as Vehicle Positions, Trip Updates, and Alerts.  
This repository is part of experiments with the **bUCR Realtime feeds** and other open transit data sources.

---

## Installation

For installation and dependency management we are using [uv](https://github.com/astral-sh/uv), a fast drop-in replacement for pip and venv.

1. **Install uv** (if you don’t already have it):

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone this repository**:

   ```bash
   git clone https://github.com/simovilab/gtfs-django.git
   cd eta-prediction
   ```

3. **Install dependencies** from `pyproject.toml`:

   ```bash
   uv sync
   ```

This will create a virtual environment and install:

- [`gtfs-realtime-bindings`](https://github.com/MobilityData/gtfs-realtime-bindings) (protobuf definitions for GTFS-RT)  
- [`requests`](https://docs.python-requests.org/)

---

## Usage

Example: fetch and print the first 10 vehicle positions.

```bash
uv run gtfs_rt_bindings_VP.py
``` 
---

## Switching Feeds

Inside each script, change the `URL` variable to any GTFS-RT VehiclePositions endpoint.

Examples:
- bUCR (default):
  ```
  https://databus.bucr.digital/feed/realtime/vehicle_positions.pb
  ```
- MBTA:
  ```
  https://cdn.mbta.com/realtime/VehiclePositions.pb
  ```

# 🚍 GTFS-RT Ingestion Pipeline

A Django + Celery + Redis workflow for ingesting **GTFS-Realtime feeds** (Vehicle Positions and Trip Updaes protobufs `.pb`) into **PostgreSQL**.  
---

## ✨ Features
- Periodic fetch of GTFS-RT feeds (VP + TU)  
- Deduplication via SHA-256 (`RawMessage` model)  
- Idempotent parse & upsert into normalized tables  
- Namespaced feed labels (e.g. `mbta:VP`, `mbta:TU`)  
- Admin UI for browsing ingested rows  
- Tested with MBTA live feeds (15s polling)  

---

## 🛠️ Stack
- **Django** → ORM, admin, migrations  
- **Celery** → task queue, periodic tasks  
- **Redis** → broker + result backend  
- **PostgreSQL** → durable storage  
- **uv** → Python project/env manager (`pyproject.toml`)  
- **gtfs-realtime-bindings** → protobuf parsing  
- **requests** → HTTP client  

---

## 📂 Project Structure
```
gtfs-rt-pipeline/
├─ pyproject.toml         # deps
├─ .env.example           # DB, Redis, feeds
├─ manage.py
├─ ingestproj/            # Django project
│  ├─ settings.py         # DB + Celery config
│  ├─ celery.py           # Celery app
└─ rt_pipeline/           # Django app
   ├─ models.py           # RawMessage, VehiclePosition, TripUpdate
   ├─ tasks.py            # fetch → parse → upsert
   ├─ admin.py            # models registered for web UI
   └─ migrations/
```

---

## ⚙️ Setup

1. **Install deps**
   ```bash
   uv sync
   ```

2. **Configure environment** (`.env`)
   Copy `.env.example` to `.env` and edit the values for your feeds/credentials.
   ```env
   # --- Django ---
   DJANGO_SECRET_KEY=change-me
   DJANGO_DEBUG=True
   DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1,web
   
   # --- Postgres ---
   DATABASE_URL=postgresql://gtfs:gtfs@postgres:5432/gtfs   # use service name inside Docker compose
   # For local tooling outside Docker override to postgresql://gtfs:gtfs@localhost:15432/gtfs
   
   # --- Redis (Celery broker/backend) ---
   REDIS_URL=redis://redis:6379/0
   # For local tooling override to redis://localhost:16379/0
   
   # --- Feed config ---
   FEED_NAME=mbta
   GTFSRT_VEHICLE_POSITIONS_URL=https://cdn.mbta.com/realtime/VehiclePositions.pb
   GTFSRT_TRIP_UPDATES_URL=https://cdn.mbta.com/realtime/TripUpdates.pb
   POLL_SECONDS=15
   
   # --- HTTP ---
   HTTP_CONNECT_TIMEOUT=3
   HTTP_READ_TIMEOUT=5
   ```

3. **Run services with Docker Compose**
   ```bash
   cd gtfs-rt-pipeline
   cp .env.example .env   # if you haven't already

   # Build images + boot Postgres & Redis first
   docker compose up -d postgres redis

   # Run migrations + create admin user
   docker compose run --rm web python manage.py migrate
   docker compose run --rm web python manage.py createsuperuser

   # Start Django + Celery worker/beat
   docker compose up -d web celery-worker celery-beat
   ```

   Logs:
   ```bash
   docker compose logs -f web
   docker compose logs -f celery-worker
   ```
   Host → container port mappings:
   - `localhost:15432` → Postgres `5432`
   - `localhost:16379` → Redis `6379`
   - `localhost:18000` → Django web `8000`

   > ℹ️ The `postgres` service uses the `postgis/postgis:16-3.4` image so the `postgis` extension is available during migrations.

---

## 🔄 How It Works

1. **Fetch task** (`fetch_vehicle_positions` / `fetch_trip_updates`)  
   - Downloads `.pb` feed → computes hash → dedup in `RawMessage`.  
   - If new → enqueues parse task.

2. **Parse task** (`parse_and_upsert_*`)  
   - Converts protobuf into rows.  
   - Bulk upserts into `VehiclePosition` or `TripUpdate`.  
   - Idempotent thanks to unique constraints.

---

## 🔍 Inspecting Data

**Django Admin**  
Ensure `docker compose up -d web` is running, then visit `http://localhost:18000/admin` (container port 8000 is mapped to host 18000).  
Need a shell inside the container?
```bash
docker compose exec web python manage.py shell
```
Log in with your superuser → browse RawMessages, Vehicle Positions, and Trip Updates.

---

## 📊 Current Status
✅ End-to-end pipeline works:  
- Live MBTA data ingested every ~15s  
- Deduplication enforced  
- VP & TU both flowing into Postgres  
- Admin UI enabled for quick inspection  

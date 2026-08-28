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
   ```env
   # --- Django ---
   DJANGO_SECRET_KEY=change-me
   DJANGO_DEBUG=True
   DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1
   
   # --- Postgres ---
   DATABASE_URL=postgresql://gtfs:gtfs@localhost:5432/gtfs
   
   # --- Redis (Celery broker/backend) ---
   REDIS_URL=redis://localhost:6379/0
   
   # --- Feed config ---
   FEED_NAME=mbta
   GTFSRT_VEHICLE_POSITIONS_URL=https://cdn.mbta.com/realtime/VehiclePositions.pb
   GTFSRT_TRIP_UPDATES_URL=https://cdn.mbta.com/realtime/TripUpdates.pb
   POLL_SECONDS=15
   
   # --- HTTP ---
   HTTP_CONNECT_TIMEOUT=3
   HTTP_READ_TIMEOUT=5
   ```

3. **Run services**
   ```bash
   # Start Redis + Postgres
   redis-server &
   postgres -D /usr/local/var/postgres &
   
   # Django
   python manage.py migrate
   python manage.py createsuperuser
   python manage.py runserver
   
   # Celery
   celery -A ingestproj worker -Q fetch,upsert -l INFO
   celery -A ingestproj beat -l INFO
   ```

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
```bash
python manage.py runserver
```
Go to `localhost:8000/admin` → log in with your superuser → browse RawMessages, Vehicle Positions, and Trip Updates.

---

## 📊 Current Status
✅ End-to-end pipeline works:  
- Live MBTA data ingested every ~15s  
- Deduplication enforced  
- VP & TU both flowing into Postgres  
- Admin UI enabled for quick inspection  


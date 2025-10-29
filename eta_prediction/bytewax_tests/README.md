# Bytewax Testing

## Install Dependencies

```bash
uv sync
``` 

## Usage

### Sample Flow (`sample_dataflow.py`)

```bash
uv run python -m bytewax.run sample_dataflow:flow
```

### Periodic Input (`periodic_input.py`)

*Stateless:*
```bash
uv run python -m bytewax.run periodic_input:stateless_flow
```

*Stateful:*
```bash
uv run python -m bytewax.run periodic_input:stateful_flow
```

### GTFS Flow (`gtfs-flow/gtfs_flow.py`)

```bash
uv run python -m bytewax.run gtfs-flow/gtfs_flow:flow
```

### MQTT + Redis Flow
```bash
uv run python -m bytewax.run bytewax_redis_flow
```
 *Multiple workers (e.g. 2):*

```bash
 uv run python -m bytewax.run bytewax_flow -w 2
```
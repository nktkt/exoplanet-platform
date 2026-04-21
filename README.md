# Exoplanet Analysis Platform

A production-grade Python platform for ingesting, storing, and analyzing exoplanet data from public astronomical archives, exposed through a REST API, a Typer CLI, and a Streamlit dashboard.

## Features

- **Ingestion** — NASA Exoplanet Archive (TAP/ADQL), MAST light curves (via `lightkurve`), JPL Horizons ephemerides, and Gaia DR3 stellar parameters.
- **Analysis** — Box Least Squares transit search (astropy), median-filter detrending, phase folding, Kepler orbital mechanics, Kopparapu habitable-zone bounds, Earth Similarity Index, and Stefan-Boltzmann stellar-property derivation.
- **Storage** — SQLAlchemy 2.0 ORM with portable schema for SQLite (dev) or PostgreSQL (prod), plus Alembic migrations scaffolding.
- **Interfaces** — FastAPI REST service, Typer CLI (`exoplanet`), and a Streamlit dashboard.
- **Ops** — structlog JSON/console logging, tenacity-based HTTP retries, cachetools TTL cache, Docker multi-stage build, GitHub Actions CI.

## Architecture

```
 Public archives              Ingestion                Storage                 Analysis                 Interfaces
 ───────────────           ─────────────            ─────────────            ─────────────            ──────────────
 NASA Exo Archive ─┐
 MAST / Kepler/TESS├──▶ ingestion/*.py ──▶ storage/repository.py ──▶ analysis/*.py ──┬─▶ api/  (FastAPI REST)
 JPL Horizons      │     (HTTPClient        (SQLAlchemy ORM +           (BLS, Kepler, │
 Gaia DR3         ─┘      + retry/cache)     Planet/Star/LightCurve)     Kopparapu)   ├─▶ cli/ (Typer)
                                                                                       └─▶ dashboard/ (Streamlit)
                           config.py / logging_config.py / exceptions.py share infrastructure
```

## Installation

```bash
# From source (recommended for development)
git clone https://github.com/your-org/exoplanet-platform.git
cd exoplanet-platform
pip install -e .[dev]

# Or via Docker Compose (API + dashboard + PostgreSQL)
docker compose up --build
```

## Quick start

```bash
# 1. Initialise the local SQLite database.
exoplanet db init

# 2. Pull a planet record from the NASA Exoplanet Archive.
exoplanet ingest "Kepler-10 b" --catalog nasa

# 3. List what's in your DB.
exoplanet list-planets

# 4. Download a TESS light curve and search it for transits.
exoplanet analyze transit "Kepler-10" --mission Kepler --min-period 0.5 --max-period 5

# 5. Start the API (http://localhost:8000/docs) or the dashboard.
exoplanet serve api
# exoplanet serve dashboard  # Streamlit on :8501
```

## CLI reference

| Command                                 | Purpose                                                  |
| --------------------------------------- | -------------------------------------------------------- |
| `exoplanet search <name>`               | Search NASA Exoplanet Archive by substring.              |
| `exoplanet get <identifier>`            | Fetch a single planet record and show a detail panel.    |
| `exoplanet ingest <id> [--catalog]`     | Fetch and upsert a planet or star (`nasa` / `gaia`).     |
| `exoplanet list-planets [--filters]`    | List stored planets with optional filters.               |
| `exoplanet orbit <body> --start --stop` | Fetch JPL Horizons ephemeris; writes CSV to `data/`.     |
| `exoplanet light-curve download`        | Download and persist a MAST light curve.                 |
| `exoplanet analyze transit <target>`    | Detrend + BLS search and print the top candidate signals.|
| `exoplanet analyze habitability <p>`    | Score a stored planet against its host star's HZ.        |
| `exoplanet db init`                     | Create all database tables (idempotent).                 |
| `exoplanet serve api`                   | Run the FastAPI service (uvicorn).                       |
| `exoplanet serve dashboard`             | Launch the Streamlit dashboard.                          |

## REST API reference

| Method | Endpoint                     | Purpose                                             |
| ------ | ---------------------------- | --------------------------------------------------- |
| GET    | `/health`                    | Liveness + per-dependency status check.             |
| GET    | `/planets`                   | Paginated listing with `host_star` / `catalog` filters. |
| GET    | `/planets/{identifier}`      | Fetch one stored planet (404 on miss).              |
| POST   | `/planets/ingest`            | Fetch a planet from an upstream catalog and upsert. |
| GET    | `/stars`                     | Paginated listing of stored stars.                  |
| GET    | `/stars/{identifier}`        | Fetch one stored star.                              |
| POST   | `/stars/ingest`              | Fetch a star (Gaia) and upsert.                     |
| POST   | `/light-curves/download`     | Download + persist a MAST light curve.              |
| GET    | `/light-curves/{id}`         | Metadata + truncated arrays for a stored curve.     |
| POST   | `/analysis/transit-search`   | Run BLS on a stored/fresh light curve.              |
| POST   | `/analysis/habitability`     | HZ + ESI assessment for a stored planet.            |

Interactive OpenAPI docs are served at `/docs` when the API is running.

## Configuration

All runtime knobs are environment variables prefixed `EXOPLANET_`. Nested sections use a double-underscore delimiter, e.g. `EXOPLANET_STORAGE__DATABASE_URL`. See [`.env.example`](./.env.example) for the full list with commented defaults.

## Development

```bash
pip install -e .[dev]          # install dev extras (pytest, ruff, mypy, ...)

pytest                          # run the fast test suite (network tests deselected)
pytest -m integration           # run only integration tests (require network)

ruff check src tests            # lint
mypy src                        # static type check
```

CI runs on Python 3.11 and 3.12 (GitHub Actions); see [`.github/workflows/ci.yml`](./.github/workflows/ci.yml).

## License

MIT — see the `pyproject.toml` classifier and SPDX identifier.

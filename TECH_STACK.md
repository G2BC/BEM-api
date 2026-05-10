# Tech Stack

## Language & Runtime

|                     |                                       |
| ------------------- | ------------------------------------- |
| **Python**          | 3.12                                  |
| **Package manager** | [uv](https://github.com/astral-sh/uv) |

## Web Framework

|                        |                                    |
| ---------------------- | ---------------------------------- |
| **Flask**              | 3.1                                |
| **flask-smorest**      | OpenAPI 3.0 / Swagger UI (`/docs`) |
| **marshmallow**        | Serialization & validation         |
| **Flask-JWT-Extended** | JWT authentication                 |
| **Flask-CORS**         | Cross-origin resource sharing      |

## Database

|                             |                    |
| --------------------------- | ------------------ |
| **PostgreSQL**              | 17                 |
| **SQLAlchemy**              | 2.0 (ORM)          |
| **Flask-SQLAlchemy**        | Flask integration  |
| **Alembic / Flask-Migrate** | Schema migrations  |
| **psycopg2**                | PostgreSQL adapter |

## Cache

|              |                     |
| ------------ | ------------------- |
| **Redis**    | 7                   |
| **redis-py** | 5.2 (Python client) |

Cached resources use configurable TTLs (default 24 h for observations and NCBI data).

## Object Storage

|                   |                             |
| ----------------- | --------------------------- |
| **MinIO**         | S3-compatible storage (dev) |
| **boto3 / minio** | Python clients              |

Buckets: temporary uploads, final assets, database backups.

## External APIs

| API                   | Usage                    |
| --------------------- | ------------------------ |
| **iNaturalist**       | Species observation sync |
| **Mushroom Observer** | Species observation sync |
| **NCBI Entrez**       | Taxonomy & genomic data  |
| **IUCN Red List**     | Conservation status      |
| **MycoBank**          | Fungal taxonomy          |
| **DeepL**             | Translation              |

## Data & Science Libraries

|               |                             |
| ------------- | --------------------------- |
| **BioPython** | NCBI Entrez integration     |
| **pandas**    | Data processing for imports |
| **openpyxl**  | Excel file parsing          |
| **numpy**     | Numerical support           |

## Application Server

|              |                             |
| ------------ | --------------------------- |
| **Gunicorn** | 23 (production WSGI server) |
| **Port**     | 9083                        |

## Infrastructure

|                    |                                   |
| ------------------ | --------------------------------- |
| **Docker**         | Python 3.12-slim base image       |
| **Docker Compose** | Dev and production configurations |

## CI/CD

|                       |                                                                  |
| --------------------- | ---------------------------------------------------------------- |
| **GitHub Actions**    | Lint, build, security scan, release                              |
| **Trivy**             | Container & filesystem vulnerability scanning                    |
| **GHCR**              | Container image registry                                         |
| **Scheduled Actions** | Automated syncs (iNaturalist, Mushroom Observer, IUCN, MycoBank) |

## Code Quality

|                |                    |
| -------------- | ------------------ |
| **ruff**       | Linter & formatter |
| **pre-commit** | Git hooks          |

## Monitoring

|            |                |
| ---------- | -------------- |
| **Sentry** | Error tracking |

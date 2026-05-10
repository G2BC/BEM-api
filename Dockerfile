FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pip>=26.0" uv

WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV UV_NO_CACHE=1
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

ENV PATH="/opt/venv/bin:$PATH"

RUN addgroup --system --gid 10001 app && adduser --system --uid 10001 --ingroup app app

COPY --chown=app:app . .

RUN sed -i "s/\r$//" entrypoint.sh && chmod +x entrypoint.sh

USER app

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD curl -fsS http://localhost:9083/health || exit 1

ENTRYPOINT ["./entrypoint.sh"]

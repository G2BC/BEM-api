#!/bin/sh
set -e

if [ "$APP_ENV" = "development" ]; then
  echo "Running database migrations..."
  uv run flask db upgrade
  echo "🚀 Starting in development mode..."
  exec uv run flask run --host=0.0.0.0 --port=4000
else
  echo "🚀 Starting in production mode..."
  exec gunicorn \
    -w 4 \
    --timeout 120 \
    --graceful-timeout 30 \
    -b 0.0.0.0:9083 \
    wsgi:app
fi

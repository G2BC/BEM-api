#!/bin/sh
set -eu

: "${MINIO_ENDPOINT:=minio:9000}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER nao definido}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD nao definido}"
: "${MINIO_ACCESS_KEY:?MINIO_ACCESS_KEY nao definido}"
: "${MINIO_SECRET_KEY:?MINIO_SECRET_KEY nao definido}"
: "${MINIO_TMP_BUCKET:?MINIO_TMP_BUCKET nao definido}"
: "${MINIO_FINAL_BUCKET:?MINIO_FINAL_BUCKET nao definido}"
: "${MINIO_DB_BUCKET:?MINIO_DB_BUCKET nao definido}"

ALIAS_NAME="local"
MINIO_URL="http://${MINIO_ENDPOINT}"
POLICY_NAME="bem-app-policy"
POLICY_FILE="/policies/app-policy.json"

attempt=0
until mc alias set "${ALIAS_NAME}" "${MINIO_URL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "${attempt}" -ge 30 ]; then
    echo "Nao foi possivel conectar ao MinIO em ${MINIO_URL}" >&2
    exit 1
  fi
  sleep 2
done

mc mb --ignore-existing "${ALIAS_NAME}/${MINIO_TMP_BUCKET}"
mc mb --ignore-existing "${ALIAS_NAME}/${MINIO_FINAL_BUCKET}"
mc mb --ignore-existing "${ALIAS_NAME}/${MINIO_DB_BUCKET}"

if ! mc admin user info "${ALIAS_NAME}" "${MINIO_ACCESS_KEY}" >/dev/null 2>&1; then
  mc admin user add "${ALIAS_NAME}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
fi

if [ ! -f "${POLICY_FILE}" ]; then
  echo "Arquivo de policy obrigatorio nao encontrado: ${POLICY_FILE}" >&2
  exit 1
fi

if mc admin policy info "${ALIAS_NAME}" "${POLICY_NAME}" >/dev/null 2>&1; then
  mc admin policy detach "${ALIAS_NAME}" "${POLICY_NAME}" --user "${MINIO_ACCESS_KEY}" >/dev/null 2>&1 || true
  if ! mc admin policy remove "${ALIAS_NAME}" "${POLICY_NAME}" >/dev/null 2>&1; then
    mc admin policy rm "${ALIAS_NAME}" "${POLICY_NAME}" >/dev/null 2>&1
  fi
fi

mc admin policy create "${ALIAS_NAME}" "${POLICY_NAME}" "${POLICY_FILE}" >/dev/null
mc admin policy attach "${ALIAS_NAME}" "${POLICY_NAME}" --user "${MINIO_ACCESS_KEY}" >/dev/null
mc anonymous set download "${ALIAS_NAME}/${MINIO_FINAL_BUCKET}" >/dev/null
mc anonymous set none "${ALIAS_NAME}/${MINIO_DB_BUCKET}" >/dev/null || true
mc anonymous set none "${ALIAS_NAME}/${MINIO_TMP_BUCKET}" >/dev/null || true

echo "MinIO configurado com sucesso."

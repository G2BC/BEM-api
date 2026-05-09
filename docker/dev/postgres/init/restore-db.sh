#!/bin/sh
set -eu

: "${POSTGRES_INIT_BACKUP_DIR:=/backups}"

resolve_backup_file() {
  if [ -f "${POSTGRES_INIT_BACKUP_DIR}/backup.dump" ]; then
    printf '%s' "${POSTGRES_INIT_BACKUP_DIR}/backup.dump"
    return 0
  fi
  return 1
}

if ! BACKUP_FILE="$(resolve_backup_file)"; then
  echo "Arquivo obrigatorio nao encontrado: ${POSTGRES_INIT_BACKUP_DIR}/backup.dump" >&2
  exit 1
fi

echo "Backup encontrado: ${BACKUP_FILE}"

case "${BACKUP_FILE}" in
  *.dump)
    echo "Restaurando dump em formato custom/tar via pg_restore..."
    if ! pg_restore \
      --clean \
      --if-exists \
      --no-owner \
      --no-privileges \
      -U "${POSTGRES_USER}" \
      -d "${POSTGRES_DB}" \
      "${BACKUP_FILE}"; then
      echo "Falha no pg_restore. O backup.dump foi gerado por versao de pg_dump incompativel com esta imagem do Postgres." >&2
      exit 1
    fi
    ;;
  *)
    echo "Formato de backup nao suportado. Use apenas backup.dump." >&2
    exit 1
    ;;
esac

echo "Restore concluido com sucesso."

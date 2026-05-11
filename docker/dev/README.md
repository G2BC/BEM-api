# Ambiente Docker de Desenvolvimento

Este ambiente sobe:

- `api` (Flask)
- `db` (PostgreSQL)
- `redis`
- `minio`
- `minio-init` (cria buckets, usuário da app e aplica policy)

## Fluxo rápido (passo a passo)

1. Copie o arquivo de ambiente:

```bash
cp .env.example .env
```

- Usaremos dotend-vault

2. Preencha no `.env` as variáveis.

3. Garanta o arquivo de policy do MinIO:

- `docker/dev/minio/policies/app-policy.json`

4. Garanta o dump do banco:

- caminho: `docker/dev/db-backup/`
- nome obrigatório: `backup.dump`
- formato suportado: `.dump`

5. Valide o compose (pega erro de env faltando antes de subir):

```bash
docker compose -f docker-compose.dev.yml config
```

6. Suba a stack:

```bash
docker compose -f docker-compose.dev.yml up --build
```

7. Valide rapidamente:

```bash
docker compose -f docker-compose.dev.yml ps
curl http://localhost:4000/health
```

## Restore do banco (como funciona)

- O restore roda automaticamente no **primeiro bootstrap** do volume `postgres_data`.
- Se o volume já existe, o restore **não** roda de novo.

Para forçar restore novamente (apaga dados atuais do Docker local):

```bash
docker compose -f docker-compose.dev.yml down -v
docker compose -f docker-compose.dev.yml up --build
```

## Erros comuns

- `app-policy.json` ausente:
  `minio-init` falha e a `api` não sobe.
- `backup.dump` ausente:
  o serviço `db` falha no script de restore.
- dump incompatível com a versão do Postgres da stack:
  gere novamente o `backup.dump` com `pg_dump` compatível.

## Endpoints locais

- API: `http://localhost:4000`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`

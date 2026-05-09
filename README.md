# LUMM-server

## Requisitos

- Python 3.12
- uv (https://docs.astral.sh/uv/getting-started/installation/)
- Docker + Docker Compose (para ambiente completo com API, DB, Redis e MinIO)

## Setup local (sem Docker)

### 1. Clonar o repositório

```bash
git clone git@github.com:G2BC/LUMM-server.git
cd LUMM-server
```

### 2. Sincronizar dependências

```bash
uv sync --dev
```

### 3. Ativar ambiente virtual (opcional)

```bash
source .venv/bin/activate       # Linux/macOS
.venv\Scripts\activate          # Windows
```

### 4. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Em produção, as variáveis são gerenciadas pelo dashboard do Dotenv Vault. O servidor
não usa `.env`; o deploy via GitHub Actions usa o `.env.vault` versionado no repositório
e a secret `DOTENV_KEY` cadastrada em `Settings > Secrets and variables > Actions`.

### 5. Executar migrações

```bash
uv run flask db upgrade
```

### 6. Rodar servidor em desenvolvimento

```bash
uv run flask run --host=0.0.0.0 --port=4000
```

## Setup com Docker (recomendado para desenvolvimento)

Para subir ambiente completo com restore de banco e bootstrap de MinIO, siga:

- [docker/dev/README.md](docker/dev/README.md)

## Dev Container (VS Code/Cursor)

1. Siga primeiro o setup de Docker em [docker/dev/README.md](docker/dev/README.md) (incluindo `app-policy.json` e `backup.dump`).
2. Abra o projeto no editor e execute: `Dev Containers: Reopen in Container`.
3. Na primeira criação do container, o comando `uv sync --dev` roda automaticamente.

## Comandos úteis (local)

### Criar nova migration

```bash
uv run flask db migrate -m "mensagem"
```

### Aplicar migrations

```bash
uv run flask db upgrade
```

### Desativar o ambiente virtual

```bash
deactivate
```

### ⚠️ Mantenha as regras de Lint e Formatação

Para garantir a consistência e a qualidade do código neste projeto, **antes de mesclar uma PR**, certifique-se de que seu código passou na ferramenta de lint e formatação (`ruff`) durante a execução da action de CI.

### 🛠️ Dicas

Rode `uv run ruff check .` e `uv run ruff format .` antes de commitar.

## 📄 Licença

Distribuído sob a Licença MIT. Veja `LICENSE` para mais informações.

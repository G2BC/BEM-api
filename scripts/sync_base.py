"""
Base para scripts de sincronização do BEM.

Fornece:
- Lock distribuído no Redis (bem:sync:{source}:lock)
- Checkpoint por run (bem:sync:{source}:done:{data})
- Agrupamento de espécies pelo campo `bem` com ordenação natural
- Pausa configurável entre grupos
- Kill switch por tempo máximo
- Para automaticamente se grupo inteiro falhar ou erro fatal (ex: IP bloqueado)

Uso — subclasse deve definir:
    source_name: str        ex: "bold", "inaturalist"
    env_prefix:  str        ex: "BOLD", "INAT"

    get_species_rows(session, bem_ids) -> list
    sync_species(row, start_time) -> (upserted, deleted, completed)
        completed=False indica kill switch interno — loop para sem marcar done/failed

    is_fatal_error(exc) -> bool   [opcional, default False]
        True para erros que devem parar tudo imediatamente (ex: IP bloqueado)
"""

import os
import re
import sys
import time
from datetime import datetime, timezone

CHECKPOINT_TTL = 60 * 60 * 25  # 25 horas — cobre re-execução no mesmo dia


def _group_sort_key(name):
    m = re.match(r"^([A-Za-z]+)(\d+)$", name or "")
    if m:
        return (m.group(1), int(m.group(2)))
    return (name or "", 0)


class SyncRunner:
    source_name: str = ""
    env_prefix: str = ""

    def __init__(self, app):
        self.app = app
        self.max_runtime = int(os.getenv(f"{self.env_prefix}_MAX_RUNTIME_SECONDS", "14100"))
        self.group_pause = int(os.getenv(f"{self.env_prefix}_GROUP_PAUSE_SECONDS", "180"))
        self._lock_key = f"bem:sync:{self.source_name}:lock"
        self._lock_ttl = self.max_runtime + 300

    # ---------------------------------------------------------------------------
    # Abstract
    # ---------------------------------------------------------------------------

    def get_species_rows(self, session, bem_ids: list) -> list:
        """Retorna lista de rows a sincronizar. Deve incluir .id e .bem."""
        raise NotImplementedError

    def sync_species(self, row, start_time: float) -> tuple[int, int, bool]:
        """Sincroniza uma espécie. Retorna (upserted, deleted, completed).
        completed=False indica que o kill switch disparou internamente."""
        raise NotImplementedError

    def is_fatal_error(self, exc: Exception) -> bool:
        """True para erros que devem interromper o sync imediatamente."""
        return False

    # ---------------------------------------------------------------------------
    # Redis
    # ---------------------------------------------------------------------------

    def _get_redis(self):
        url = os.getenv("REDIS_URL", "").strip()
        if not url:
            return None
        try:
            import redis
            client = redis.from_url(url, decode_responses=True, socket_timeout=3)
            client.ping()
            return client
        except Exception as exc:
            self._log(f"[AVISO] Redis indisponível — checkpoint desativado: {exc}")
            return None

    def _acquire_lock(self, r):
        if not r:
            return True
        return bool(r.set(self._lock_key, "1", nx=True, ex=self._lock_ttl))

    def _release_lock(self, r):
        if r:
            r.delete(self._lock_key)

    def _done_key(self, run_date):
        return f"bem:sync:{self.source_name}:done:{run_date}"

    def _failed_key(self, run_date):
        return f"bem:sync:{self.source_name}:failed:{run_date}"

    def _is_done(self, r, run_date, species_id):
        if not r:
            return False
        try:
            return bool(r.sismember(self._done_key(run_date), str(species_id)))
        except Exception:
            return False

    def _mark_done(self, r, run_date, species_id):
        if not r:
            return
        try:
            key = self._done_key(run_date)
            r.sadd(key, str(species_id))
            r.expire(key, CHECKPOINT_TTL)
        except Exception:
            pass

    def _mark_failed(self, r, run_date, species_id):
        if not r:
            return
        try:
            key = self._failed_key(run_date)
            r.sadd(key, str(species_id))
            r.expire(key, CHECKPOINT_TTL)
        except Exception:
            pass

    # ---------------------------------------------------------------------------
    # Logging
    # ---------------------------------------------------------------------------

    def _log(self, msg):
        print(msg, flush=True)

    # ---------------------------------------------------------------------------
    # Processing
    # ---------------------------------------------------------------------------

    def _process_group(self, group_name, species_rows, run_date, r, start_time) -> bool:
        """Processa um grupo sequencialmente. Retorna True se o sync deve parar."""
        from app.extensions import db

        attempted = 0
        failed = 0
        total_upserted = 0
        total_deleted = 0

        for row in species_rows:
            if self._is_done(r, run_date, row.id):
                self._log(f"  [{row.id}] checkpoint — já processado hoje, pulando")
                continue

            if time.time() - start_time > self.max_runtime:
                self._log(f"  [{row.id}] Kill switch — abortando")
                break

            attempted += 1

            try:
                upserted, deleted, completed = self.sync_species(row, start_time)
                db.session.remove()

                if not completed:
                    # Kill switch disparou dentro de sync_species
                    self._log(f"  [{row.id}] Kill switch interno — encerrando")
                    break

                total_upserted += upserted
                total_deleted += deleted
                self._mark_done(r, run_date, row.id)
                self._log(
                    f"[OK] {group_name} species={row.id} "
                    f"upsert={upserted} removidos={deleted}"
                )
            except Exception as exc:
                db.session.remove()
                if self.is_fatal_error(exc):
                    self._log(f"[FATAL] {group_name} species={row.id}: {exc}")
                    self._mark_failed(r, run_date, row.id)
                    return True
                failed += 1
                self._log(f"[ERRO] {group_name} species={row.id}: {exc}")
                self._mark_failed(r, run_date, row.id)

        self._log(
            f"Grupo {group_name}: upsert={total_upserted} removidos={total_deleted} "
            f"falhas={failed}/{attempted}"
        )
        return attempted > 0 and failed == attempted

    def run(self):
        start_time = time.time()
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        raw_bem_ids = os.getenv("BEM_ID", "")
        bem_ids = [int(v) for raw in raw_bem_ids.split(",") if (v := raw.strip()).isdigit()]

        self._log(f"=== Sync {self.source_name} ===")
        self._log(f"limite={self.max_runtime}s | pausa_entre_grupos={self.group_pause}s")

        r = self._get_redis()

        if not self._acquire_lock(r):
            self._log("[ABORT] Outra execução já está em andamento (lock Redis ativo)")
            sys.exit(1)

        try:
            with self.app.app_context():
                from app.extensions import db

                species_rows = self.get_species_rows(db.session, bem_ids)
                self._log(f"Espécies: {len(species_rows)}")

                if bem_ids:
                    self._log("Modo manual — sem agrupamento por bem")
                    self._process_group("manual", species_rows, run_date, r, start_time)
                else:
                    groups: dict[str, list] = {}
                    for row in species_rows:
                        group = row.bem or "sem_grupo"
                        groups.setdefault(group, []).append(row)

                    sorted_group_names = sorted(groups.keys(), key=_group_sort_key)
                    self._log(f"Grupos: {sorted_group_names}")

                    for i, group_name in enumerate(sorted_group_names):
                        if time.time() - start_time > self.max_runtime:
                            self._log("[KILL SWITCH] Tempo máximo atingido")
                            break

                        group_species = groups[group_name]
                        self._log(f"\n=== Grupo {group_name} ({len(group_species)} espécies) ===")

                        should_stop = self._process_group(
                            group_name, group_species, run_date, r, start_time
                        )

                        if should_stop:
                            self._log(f"[STOP] Grupo {group_name} — encerrando sync")
                            break

                        if i < len(sorted_group_names) - 1:
                            self._log(f"Pausa de {self.group_pause}s antes do próximo grupo...")
                            time.sleep(self.group_pause)

        finally:
            self._release_lock(r)

        self._log(f"Tempo total: {int(time.time() - start_time)}s")

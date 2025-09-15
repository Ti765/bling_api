#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import base64
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter

# =========================
# Constantes API (hostname corrigido)
# =========================
API_BASE   = "https://api.bling.com.br/Api/v3"
TOKEN_URL  = f"{API_BASE}/oauth/token"
NFE_URL    = f"{API_BASE}/nfe"

USER_AGENT = "glip-nfe-extractor/1.2"

# ---- Knobs de performance (HARD CODED) ----
TARGET_RPS_INIT     = 2.75   # (ajuste) alvo inicial
JITTER_MAX_SECONDS  = 0.03   # jitter máx ~30ms por req
ADAPT_WINDOW        = 150    # (ajuste) janela para controle adaptativo (antes 200)
ADAPT_LOW_429       = 0.005  # <=0,5%: sobe
ADAPT_HIGH_429      = 0.010  # >1%: desce
ADAPT_UP_STEP       = 0.10   # +0.1 rps (até o teto)
ADAPT_RPS_MAX       = 3.0    # teto de rps
ADAPT_DOWN_FACTOR   = 0.90   # -10% rps ao ultrapassar HIGH
RETRY_PASSES_COOLDOWN_BASE = 60   # s (cooldown inicial entre passes)
RETRY_PASSES_COOLDOWN_MAX  = 300  # s (máximo)

# =========================
# Utilidades
# =========================
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

def ensure_dir(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def parse_date_yyyy_mm_dd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)

def bling_datetime(dt: datetime, end_of_day: bool = False) -> str:
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=0)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================
# Observabilidade + Rate Limiter
# =========================
class Stats:
    def __init__(self):
        self.start_ts = time.time()
        self.req_total = 0
        self.req_ok = 0
        self.req_4xx = 0
        self.req_5xx = 0
        self.req_429 = 0
        self.req_401_403 = 0
        self.lat_total = 0.0
        self.pages = 0
        self.details = 0
        self._lat_samples: List[float] = []
        self._lat_max_samples = 4000
        self._codes_window: List[int] = []

    def record(self, status: int, latency: float):
        self.req_total += 1
        self.lat_total += latency
        if len(self._lat_samples) < self._lat_max_samples:
            self._lat_samples.append(latency)
        else:
            idx = random.randint(0, self._lat_max_samples - 1)
            self._lat_samples[idx] = latency

        self._codes_window.append(status)
        if 200 <= status < 300:
            self.req_ok += 1
        elif status == 429:
            self.req_429 += 1
        elif status in (401, 403):
            self.req_401_403 += 1
        elif 400 <= status < 500:
            self.req_4xx += 1
        elif 500 <= status < 600:
            self.req_5xx += 1

    @staticmethod
    def _percentile(data: List[float], p: float) -> float:
        if not data:
            return 0.0
        data = sorted(data)
        k = (len(data) - 1) * p
        f = int(k)
        c = min(f + 1, len(data) - 1)
        if f == c:
            return data[int(k)]
        d0 = data[f] * (c - k)
        d1 = data[c] * (k - f)
        return d0 + d1

    def lat_p95(self) -> float:
        return self._percentile(self._lat_samples, 0.95)

    def lat_p99(self) -> float:
        return self._percentile(self._lat_samples, 0.99)

    def window_429_rate(self) -> float:
        if not self._codes_window:
            return 0.0
        return sum(1 for c in self._codes_window if c == 429) / len(self._codes_window)

    def reset_window(self):
        self._codes_window.clear()

    def summary(self) -> str:
        elapsed = max(1e-9, time.time() - self.start_ts)
        avg_lat = (self.lat_total / self.req_total) if self.req_total else 0.0
        eff_rps = self.req_total / elapsed
        return (
            f"=== Métricas ===\n"
            f"Tempo total: {elapsed:.1f}s\n"
            f"Req total: {self.req_total} | OK: {self.req_ok} | 4xx: {self.req_4xx} "
            f"| 401/403: {self.req_401_403} | 429: {self.req_429} | 5xx: {self.req_5xx}\n"
            f"Latência média: {avg_lat:.3f}s | p95: {self.lat_p95():.3f}s | p99: {self.lat_p99():.3f}s | RPS efetivo: {eff_rps:.2f}\n"
            f"Páginas listadas: {self.pages} | Notas detalhadas: {self.details}"
        )

class RateLimiter:
    """
    Throttling ~2.75 rps com jitter e penalidade dinâmica.
    Controle adaptativo por janela (150 req).
    """
    def __init__(self, stats: Stats):
        self.stats = stats
        self.target_rps = TARGET_RPS_INIT
        self.base_interval = 1.0 / self.target_rps
        self.jitter_max = JITTER_MAX_SECONDS
        self._last = 0.0
        self.penalty_factor = 1.0
        self.success_since_penalty = 0

    def _recalc_interval(self):
        self.base_interval = 1.0 / max(0.1, self.target_rps)

    def wait(self):
        now = time.time()
        interval = self.base_interval * self.penalty_factor
        wait_needed = (self._last + interval) - now
        if wait_needed > 0:
            time.sleep(wait_needed + random.uniform(0.0, self.jitter_max))
            now = time.time()
        self._last = now
        # (ajuste) relaxa penalidade com mais frequência: 6 sucessos
        self.success_since_penalty += 1
        if self.success_since_penalty >= 6 and self.penalty_factor > 1.0:
            self.penalty_factor = max(1.0, self.penalty_factor * 0.9)
            self.success_since_penalty = 0

        # controle adaptativo por janela
        if self.stats.req_total > 0 and (self.stats.req_total % ADAPT_WINDOW == 0):
            rate_429 = self.stats.window_429_rate()
            if rate_429 <= ADAPT_LOW_429 and self.target_rps < ADAPT_RPS_MAX:
                new_rps = min(ADAPT_RPS_MAX, self.target_rps + ADAPT_UP_STEP)
                if new_rps != self.target_rps:
                    self.target_rps = new_rps
                    self._recalc_interval()
                    log(f"[ADAPT] 429-rate={rate_429:.3%} ⇒ aumentando TARGET_RPS para {self.target_rps:.2f}")
            elif rate_429 > ADAPT_HIGH_429:
                self.target_rps = max(0.5, self.target_rps * ADAPT_DOWN_FACTOR)
                self._recalc_interval()
                self.penalty_factor = min(4.0, self.penalty_factor * 1.25)  # leve ajuste ao descer
                log(f"[ADAPT] 429-rate={rate_429:.3%} ⇒ reduzindo TARGET_RPS para {self.target_rps:.2f}")
            self.stats.reset_window()

    def penalize(self, retry_after_s: Optional[float] = None):
        if retry_after_s and retry_after_s > 0:
            time.sleep(retry_after_s)
        # (ajuste) penalidade mais suave: ×1.25 (antes ×1.5), teto 4x
        self.penalty_factor = min(4.0, self.penalty_factor * 1.25)
        self.success_since_penalty = 0

# =========================
# .env helpers (mantidos)
# =========================
def load_env_file(env_path: str) -> None:
    if not env_path or not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and os.getenv(k) is None:
                os.environ[k] = v

def update_env_value(env_path: str, key: str, value: str) -> None:
    lines = []
    found = False
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    new_lines = []
    for ln in lines:
        if ln.strip().startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            found = True
        else:
            new_lines.append(ln)
    if not found:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines[-1] = new_lines[-1] + "\n"
        new_lines.append(f"{key}={value}\n")
    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

# =========================
# OAuth helpers (inalterados na lógica)
# =========================
def _basic(client_id: str, client_secret: str) -> str:
    return base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

def token_from_refresh(client_id: str, client_secret: str, refresh_token: str) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "1.0",
        "Authorization": f"Basic {_basic(client_id, client_secret)}",
        "User-Agent": USER_AGENT,
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": resp.text}
    if resp.status_code != 200 or "access_token" not in payload:
        raise RuntimeError(f"Falha no refresh_token (status {resp.status_code}): {payload}")
    return payload

def tokens_from_auth_code(client_id: str, client_secret: str, authorization_code: str) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "1.0",
        "Authorization": f"Basic {_basic(client_id, client_secret)}",
        "User-Agent": USER_AGENT,
    }
    data = {"grant_type": "authorization_code", "code": authorization_code.strip()}
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": resp.text}
    if resp.status_code != 200 or "access_token" not in payload:
        raise RuntimeError(f"Falha ao trocar authorization_code (status {resp.status_code}): {payload}")
    return payload

def persist_tokens_json(path: str, tokens: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)
    log(f"Tokens salvos em {path}.")

def read_refresh_from_json(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("refresh_token")
    except Exception:
        return None

def ensure_access_token() -> Dict[str, Any]:
    client_id     = os.getenv("BLING_CLIENT_ID") or ""
    client_secret = os.getenv("BLING_CLIENT_SECRET") or ""
    if not client_id or not client_secret:
        raise RuntimeError("BLING_CLIENT_ID/BLING_CLIENT_SECRET ausentes.")

    save_mode  = (os.getenv("BLING_SAVE_TOKENS") or "json").lower()  # json|env
    env_path   = os.getenv("BLING_ENV_FILE_PATH") or ".env"
    token_file = os.getenv("BLING_TOKEN_FILE") or "./tokens.json"

    refresh = os.getenv("BLING_REFRESH_TOKEN")
    if not refresh and os.path.exists(token_file):
        refresh = read_refresh_from_json(token_file)

    if refresh:
        tokens = token_from_refresh(client_id, client_secret, refresh)
    else:
        auth_code = os.getenv("BLING_AUTHORIZATION_CODE")
        if not auth_code:
            raise RuntimeError("Informe BLING_REFRESH_TOKEN ou BLING_AUTHORIZATION_CODE.")
        tokens = tokens_from_auth_code(client_id, client_secret, auth_code)

    new_refresh = tokens.get("refresh_token")
    if save_mode == "env":
        update_env_value(env_path, "BLING_REFRESH_TOKEN", new_refresh or "")
        os.environ["BLING_REFRESH_TOKEN"] = new_refresh or ""
        log(f"Refresh token atualizado no {env_path}.")
    else:
        persist_tokens_json(token_file, tokens)

    return tokens

def make_session(access_token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    })
    # Pool de conexões/keep-alive
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# =========================
# HTTP com retry/backoff + limiter centralizado
# =========================
def http_get(sess: requests.Session,
             url: str,
             params: Optional[Dict[str, Any]] = None,
             max_retries: int = 5,
             limiter: Optional[RateLimiter] = None,
             stats: Optional[Stats] = None) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            if limiter:
                limiter.wait()
            t0 = time.time()
            resp = sess.get(url, params=params, timeout=60)
            latency = time.time() - t0

            sc = resp.status_code
            if stats:
                stats.record(sc, latency)

            if sc == 429:
                ra = resp.headers.get("Retry-After")
                # (ajuste) fallback mais curto: min(1*attempt, 6s)
                retry_after = float(ra) if (ra and ra.isdigit()) else min(1.0 * attempt, 6.0)
                log(f"[429] Esperando {retry_after:.1f}s (tentativa {attempt}/{max_retries})...")
                if limiter:
                    limiter.penalize(retry_after_s=retry_after)
                else:
                    time.sleep(retry_after)
                # garante que, se só der 429, a exceção final carregue "429" para entrar na retry_queue
                last_err = RuntimeError("HTTP 429: too many requests")
                continue

            if sc in (401, 403):
                raise PermissionError(f"Auth erro {sc}: {resp.text}")

            if 400 <= sc < 500:
                raise RuntimeError(f"Erro {sc}: {resp.text}")

            if 500 <= sc < 600:
                raise RuntimeError(f"Erro 5xx {sc}: {resp.text}")

            ct = resp.headers.get("Content-Type", "")
            data = resp.json() if ct.startswith("application/json") else {}
            return data

        except Exception as e:
            last_err = e
            wait = min(1.0 * attempt, 5.0)  # (ajuste) backoff mais curto
            log(f"[GET retry {attempt}/{max_retries}] {e} -> aguardando {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(f"Falha GET {url}: {last_err}")

# =========================
# API: listar e detalhar NF-e (lógica mantida)
# =========================
def listar_nfe_ids(sess: requests.Session,
                   dt_ini: datetime,
                   dt_fim: datetime,
                   situacao: Optional[int] = 5,
                   limite: int = 100,
                   max_paginas: int = 10000,
                   limiter: Optional[RateLimiter] = None,
                   stats: Optional[Stats] = None) -> List[int]:
    ids_set: Set[int] = set()
    pagina = 1
    paginas_sem_novos = 0

    while pagina <= max_paginas:
        params = {
            "tipo": "1",
            "dataEmissaoInicial": bling_datetime(dt_ini, end_of_day=False),
            "dataEmissaoFinal":   bling_datetime(dt_fim,  end_of_day=True),
            "limite": limite,
            "pagina": pagina,
        }
        if situacao is not None:
            params["situacao"] = situacao

        payload = http_get(sess, NFE_URL, params, limiter=limiter, stats=stats)
        data = payload.get("data") or []
        if stats:
            stats.pages += 1

        if not data:
            break

        antes = len(ids_set)
        for row in data:
            nid = row.get("id")
            if isinstance(nid, int):
                ids_set.add(nid)
        novos = len(ids_set) - antes
        log(f"Página {pagina}: itens={len(data)}, novos={novos}, únicos_acum={len(ids_set)}")

        if len(data) < limite:
            break

        if novos == 0:
            paginas_sem_novos += 1
            if paginas_sem_novos >= 2:
                log("[AVISO] Páginas repetidas/sem novos IDs. Encerrando paginação para evitar loop.")
                break
        else:
            paginas_sem_novos = 0

        pagina += 1

    return sorted(ids_set)

def buscar_nfe_detalhe(sess: requests.Session,
                       nota_id: int,
                       limiter: Optional[RateLimiter] = None,
                       stats: Optional[Stats] = None) -> Dict[str, Any]:
    url = f"{NFE_URL}/{nota_id}"
    payload = http_get(sess, url, limiter=limiter, stats=stats)
    return payload.get("data") or {}

# =========================
# Execução principal (lógica mantida; retry passes + JSONL)
# =========================
def main():
    # 1) Carrega .env (somente infos necessárias)
    env_path = os.getenv("BLING_ENV_FILE_PATH") or ".env"
    load_env_file(env_path)

    company = os.getenv("BLING_COMPANY_NAME") or "AURA"
    out_dir = os.getenv("OUTPUT_DIR") or "./out"
    ensure_dir(out_dir)

    inicio = os.getenv("NFE_INICIO")  # YYYY-MM-DD
    fim    = os.getenv("NFE_FIM")     # YYYY-MM-DD
    if not inicio or not fim:
        raise RuntimeError("Defina NFE_INICIO e NFE_FIM no .env (YYYY-MM-DD).")
    dt_ini = parse_date_yyyy_mm_dd(inicio)
    dt_fim = parse_date_yyyy_mm_dd(fim)
    if dt_fim < dt_ini:
        raise RuntimeError("NFE_FIM anterior a NFE_INICIO.")

    # Default: autorizadas (5). Se quiser TODAS, deixe NFE_SITUACAO vazio no .env
    sit_env = (os.getenv("NFE_SITUACAO") or "").strip()
    situacao = int(sit_env) if (sit_env.isdigit()) else 5

    # 2) Autentica (inalterado)
    log(f"Autenticando empresa '{company}'...")
    tokens = ensure_access_token()
    access_token = tokens["access_token"]
    sess = make_session(access_token)

    # Rate limiter e métricas
    stats = Stats()
    limiter = RateLimiter(stats)

    # 3) Listagem (EXATAMENTE como antes)
    log(f"Listando NF-e de {dt_ini.date()} até {dt_fim.date()}...")
    ids = listar_nfe_ids(sess, dt_ini, dt_fim, situacao=situacao, limite=100, limiter=limiter, stats=stats)
    log(f"Total de notas no período (IDs únicos): {len(ids)}")

    # 4) Detalhes — primeiro passe sequencial (EXATAMENTE como antes) + retry queue
    stamp_ini = dt_ini.strftime("%d%m%Y")
    stamp_fim = dt_fim.strftime("%d%m%Y")
    base_name = f"{company}_{stamp_ini}_{stamp_fim}"
    out_dir_abs = os.path.abspath(out_dir)
    jsonl_path = os.path.join(out_dir_abs, base_name + ".jsonl")
    meta_path  = os.path.join(out_dir_abs, base_name + ".meta.json")
    retry_path = os.path.join(out_dir_abs, base_name + ".retry_queue.json")

    total = len(ids)
    escritos = 0
    retry_queue: List[int] = []

    with open(jsonl_path, "w", encoding="utf-8") as jf:
        for i, nid in enumerate(ids, 1):
            log(f"Detalhes {i}/{total} (id={nid})")
            try:
                det = buscar_nfe_detalhe(sess, nid, limiter=limiter, stats=stats)
                if det:
                    jf.write(json.dumps(det, ensure_ascii=False) + "\n")
                    escritos += 1
                    stats.details += 1
            except Exception as e:
                msg = str(e)
                # refileira apenas erros transitórios
                if "429" in msg or "5xx" in msg:
                    retry_queue.append(nid)
                    log(f"[RETRY-QUEUE] Nota {nid} agendada para reprocesso: {e}")
                else:
                    # 4xx≠429 e auth => falha "real": interrompe como antes
                    raise

    # 5) Passes adicionais até esvaziar retry_queue (garantia 100/100 em transitórios)
    cooldown = RETRY_PASSES_COOLDOWN_BASE
    pass_num = 1
    while retry_queue:
        with open(retry_path, "w", encoding="utf-8") as rf:
            json.dump({"pending": retry_queue}, rf, ensure_ascii=False, indent=2)
        log(f"[RETRY-PASS {pass_num}] {len(retry_queue)} notas pendentes. Cooldown {cooldown}s antes de recomeçar.")
        time.sleep(cooldown)
        next_queue: List[int] = []
        with open(jsonl_path, "a", encoding="utf-8") as jf:
            for idx, nid in enumerate(retry_queue, 1):
                log(f"[RETRY-PASS {pass_num}] Nota {idx}/{len(retry_queue)} (id={nid})")
                try:
                    det = buscar_nfe_detalhe(sess, nid, limiter=limiter, stats=stats)
                    if det:
                        jf.write(json.dumps(det, ensure_ascii=False) + "\n")
                        escritos += 1
                        stats.details += 1
                except Exception as e:
                    msg = str(e)
                    if "429" in msg or "5xx" in msg:
                        next_queue.append(nid)
                        limiter.penalize()
                        log(f"[RETRY-PASS {pass_num}] Reagendando nota {nid}: {e}")
                    else:
                        raise
        retry_queue = next_queue
        pass_num += 1
        cooldown = min(RETRY_PASSES_COOLDOWN_MAX, cooldown + 60)

    # 6) Metadados + resumo
    meta = {
        "empresa": company,
        "periodo": {"inicio": inicio, "fim": fim},
        "gerado_em": datetime.now().isoformat(),
        "total_notas_ids": total,
        "total_detalhes_escritos": escritos,
        "formato": "jsonl",
        "arquivo_detalhes": jsonl_path,
    }
    with open(meta_path, "w", encoding="utf-8") as mf:
        json.dump(meta, mf, ensure_ascii=False, indent=2)

    log(f"Arquivo de detalhes (JSONL): {jsonl_path}")
    log(f"Arquivo de metadados: {meta_path}")
    log(stats.summary())

if __name__ == "__main__":
    main()

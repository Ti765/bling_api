#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json
import time
import random
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, Tuple, List
import requests
from requests.adapters import HTTPAdapter

USER_AGENT = "glip-nfe-xml-downloader/1.0"

# =========================
# Parâmetros (fixos, hard coded)
# =========================
TARGET_RPS_START = 2.7         # alvo inicial
TARGET_RPS_MAX   = 3.0         # teto
RPS_JITTER_MAX   = 0.03        # ±30ms
DETAIL_WORKERS   = 3           # workers para download
HTTP_POOL_MAX    = 50          # pool de conexões
ADJUST_WINDOW    = 200         # a cada 200 req, reavalia RPS
ADAPT_LOWER_429_RATE = 0.005   # <0,5% -> sobe RPS
ADAPT_UPPER_429_RATE = 0.01    # >1%   -> desce RPS
ADAPT_UP_STEP        = 0.1     # passo de subida
ADAPT_DOWN_MULT      = 0.90    # multiplicador p/ queda

# Retries em "passes" (fila é reprocessada até esvaziar)
INITIAL_PASS_COOLDOWN = 60     # 60s entre passes
MAX_PASS_COOLDOWN     = 300    # até 5min

# =========================
# Utilidades
# =========================
def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

def ensure_dir(d: str) -> None:
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def sanitize_filename(name: str) -> str:
    keep = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
    return "".join(c if c in keep else "_" for c in name)

def extract_param(url: str, key: str) -> Optional[str]:
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        v = qs.get(key)
        return v[0] if v else None
    except Exception:
        return None

# =========================
# Observabilidade + Rate Limit adaptativo
# =========================
class Stats:
    def __init__(self):
        self.start_ts = time.time()
        self.req_total = 0
        self.req_ok = 0
        self.req_429 = 0
        self.req_4xx = 0
        self.req_5xx = 0
        self.bytes = 0

    def record(self, status: int, nbytes: int = 0):
        self.req_total += 1
        self.bytes += max(0, nbytes)
        if 200 <= status < 300:
            self.req_ok += 1
        elif status == 429:
            self.req_429 += 1
        elif 400 <= status < 500:
            self.req_4xx += 1
        elif 500 <= status < 600:
            self.req_5xx += 1

    def snap(self) -> Tuple[int, int]:
        return self.req_total, self.req_429

    def summary(self) -> str:
        elapsed = max(1e-9, time.time() - self.start_ts)
        rps = self.req_total / elapsed
        mb = self.bytes / (1024*1024)
        return (f"=== Métricas ===\n"
                f"Tempo: {elapsed:.1f}s | RPS efetivo: {rps:.2f}\n"
                f"Req: total={self.req_total} ok={self.req_ok} 429={self.req_429} "
                f"4xx={self.req_4xx} 5xx={self.req_5xx}\n"
                f"Transferido: {mb:.2f} MiB")

class RateLimiter:
    """RPS alvo com jitter + penalty simples."""
    def __init__(self, target_rps: float, jitter_max: float):
        self.target_rps = max(0.1, target_rps)
        self.jitter_max = max(0.0, jitter_max)
        self.base_interval = 1.0 / self.target_rps
        self._last = 0.0
        self.penalty_factor = 1.0
        self.success_since_penalty = 0

    def wait(self):
        now = time.time()
        interval = self.base_interval * self.penalty_factor
        wait_needed = (self._last + interval) - now
        if wait_needed > 0:
            time.sleep(wait_needed + random.uniform(0.0, self.jitter_max))
            now = time.time()
        self._last = now
        self.success_since_penalty += 1
        if self.success_since_penalty >= 20 and self.penalty_factor > 1.0:
            self.penalty_factor = max(1.0, self.penalty_factor * 0.9)
            self.success_since_penalty = 0

    def penalize(self, retry_after_s: Optional[float] = None):
        if retry_after_s and retry_after_s > 0:
            time.sleep(retry_after_s)
        self.penalty_factor = min(4.0, self.penalty_factor * 1.5)
        self.success_since_penalty = 0

    def set_target(self, target_rps: float):
        self.target_rps = max(0.1, target_rps)
        self.base_interval = 1.0 / self.target_rps

def adaptive_adjust(limiter: RateLimiter, stats: Stats,
                    last_window: Tuple[int, int], window_size: int) -> Tuple[int, int]:
    """A cada 'window_size' req, ajusta o target_rps usando taxa de 429."""
    total0, r429_0 = last_window
    total, r429 = stats.snap()
    if total - total0 < window_size:
        return last_window  # ainda não atingiu janela

    window_total = total - total0
    window_429   = r429 - r429_0
    rate = (window_429 / window_total) if window_total else 0.0

    new_rps = limiter.target_rps
    if rate < ADAPT_LOWER_429_RATE and new_rps < TARGET_RPS_MAX:
        new_rps = min(TARGET_RPS_MAX, new_rps + ADAPT_UP_STEP)
        limiter.set_target(new_rps)
        log(f"[ADAPT] 429_rate {rate:.2%} < {ADAPT_LOWER_429_RATE:.2%} -> sobe RPS para {new_rps:.2f}")
    elif rate > ADAPT_UPPER_429_RATE:
        new_rps = max(0.5, new_rps * ADAPT_DOWN_MULT)
        limiter.set_target(new_rps)
        log(f"[ADAPT] 429_rate {rate:.2%} > {ADAPT_UPPER_429_RATE:.2%} -> reduz RPS para {new_rps:.2f}")

    return stats.snap()  # reinicia janela

# =========================
# HTTP
# =========================
def make_session(pool_max: int = HTTP_POOL_MAX) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    adapter = HTTPAdapter(pool_connections=pool_max, pool_maxsize=pool_max, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def safe_bling_url(u: str) -> bool:
    try:
        net = urllib.parse.urlsplit(u).netloc.lower()
        return net.endswith("bling.com.br") or net.endswith("www.bling.com.br")
    except Exception:
        return False

def download_xml(sess: requests.Session, limiter: RateLimiter, url: str, out_dir: str) -> Tuple[bool, Optional[str], str]:
    """Retorna (ok, path, motivo_erro)."""
    if not url or not safe_bling_url(url):
        return False, None, "url_invalida"

    # nome do arquivo: usa chaveAcesso se existir; senão, hash simples da URL
    chave = extract_param(url, "chaveAcesso")
    fname = (chave if chave else sanitize_filename(url)) + ".xml"
    out_path = os.path.join(out_dir, fname)

    # se já existe, considera sucesso (idempotente)
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return True, out_path, "ja_existia"

    attempts = 0
    while attempts < 5:
        attempts += 1
        try:
            limiter.wait()
            resp = sess.get(url, timeout=60)
            sc = resp.status_code

            if sc == 429:
                ra = resp.headers.get("Retry-After")
                retry_after = float(ra) if (ra and ra.isdigit()) else min(2.0 * attempts, 10.0)
                limiter.penalize(retry_after_s=retry_after)
                time.sleep(retry_after)
                continue

            if sc >= 500:
                time.sleep(min(2.0 * attempts, 8.0))
                continue

            if sc != 200:
                return False, None, f"http_{sc}"

            data = resp.content or b""
            if not data:
                return False, None, "vazio"

            # grava atômico
            tmp = out_path + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, out_path)
            return True, out_path, "ok"
        except Exception as e:
            time.sleep(min(2.0 * attempts, 8.0))

    return False, None, "excedeu_retries"

# =========================
# Pipeline
# =========================
def iter_jsonl(jsonl_path: str):
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                yield json.loads(ln)
            except Exception:
                continue

def collect_xml_links(jsonl_path: str) -> List[str]:
    urls: List[str] = []
    for obj in iter_jsonl(jsonl_path):
        url = obj.get("xml") or obj.get("linkXml")  # por via das dúvidas
        if isinstance(url, str) and url.startswith("http"):
            urls.append(url)
    return urls

def main():
    if len(sys.argv) < 2:
        print("Uso: python baixar_xmls_por_jsonl.py <arquivo.jsonl> [pasta_saida]")
        sys.exit(2)

    jsonl_path = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) >= 3 else "./xml"
    ensure_dir(out_dir)

    urls = collect_xml_links(jsonl_path)
    if not urls:
        log("Nenhum link XML encontrado no JSONL.")
        sys.exit(0)

    log(f"Total de links XML: {len(urls)}")
    sess = make_session(HTTP_POOL_MAX)
    limiter = RateLimiter(TARGET_RPS_START, RPS_JITTER_MAX)
    stats = Stats()

    last_window = stats.snap()
    results: List[Tuple[str, bool, str]] = []
    retry_queue: List[str] = []

    # Passo 1: primeira varredura
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
        futs = {ex.submit(download_xml, sess, limiter, u, out_dir): u for u in urls}
        for fut in as_completed(futs):
            url = futs[fut]
            ok, path, reason = fut.result()
            if ok:
                # status 200 já contado implicitamente na próxima janela (não temos sc aqui; métrica aproximada via req_total)
                pass
            else:
                retry_queue.append(url)
            results.append((url, ok, reason))
            # ajuste adaptativo por janela
            last_window = adaptive_adjust(limiter, stats, last_window, ADJUST_WINDOW)

    # Passos adicionais (fila)
    cooldown = INITIAL_PASS_COOLDOWN
    pass_n = 1
    while retry_queue and cooldown <= MAX_PASS_COOLDOWN:
        pass_n += 1
        log(f"[Passe {pass_n}] {len(retry_queue)} pendentes. Cooldown {cooldown}s…")
        time.sleep(cooldown)
        pending = retry_queue
        retry_queue = []
        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
            futs = {ex.submit(download_xml, sess, limiter, u, out_dir): u for u in pending}
            for fut in as_completed(futs):
                url = futs[fut]
                ok, path, reason = fut.result()
                if not ok:
                    retry_queue.append(url)
                results.append((url, ok, reason))
                last_window = adaptive_adjust(limiter, stats, last_window, ADJUST_WINDOW)
        cooldown = min(MAX_PASS_COOLDOWN, cooldown + 60)

    # Relatório final
    ok_count = sum(1 for _, ok, _ in results if ok)
    fail = [(u, r) for (u, ok, r) in results if not ok]
    log(f"XMLs baixados: {ok_count}/{len(results)}  |  Falhas: {len(fail)}")
    if fail:
        for u, r in fail[:20]:
            log(f"  - FAIL: {r} :: {u}")
        if len(fail) > 20:
            log(f"  … e mais {len(fail)-20} falhas.")
    # Métricas simples (aprox. de RPS pelo tempo total do processo)
    log(stats.summary())

if __name__ == "__main__":
    main()

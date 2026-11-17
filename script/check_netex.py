# -*- coding: utf-8 -*-
"""
check_netex.py - Monitoraggio snapshot NeTEX (CCISS / MaaS4Italy), livelli 1 e 2, e test di build in OpenTripPlanner.

Per ogni regione in config.json (snapshot_base es. NUT-ITF3, levels [1, 2], cartella OTP, feed_id):
  1. interroga https://www.cciss.it/nap/public/api/v1/netex/db/snapshot
  2. per ogni livello confronta modificationTime con state.json (un livello mai visto parte subito)
  3. per ogni nuova versione: download (verifica SHA-256) e analisi del feed in streaming (netex_operators.py):
     operatori, linee, corse, corse attive nei prossimi 7 giorni, corse attive "a 2-3 settimane" (giorni 14-20),
     corse scadute; avvisi: operatore sparito / calo corse attive rispetto alla versione precedente dello stesso
     livello, orari scaduti, orari in scadenza
  4. baseline di regione (baseline.json) sulle corse attive a 2-3 settimane, condivisa tra i livelli: impostata
     automaticamente se assente, sale da sola se il feed cresce oltre baseline_warn_pct, non scende mai
  5. build OTP di ogni livello nuovo (test dell'ultima versione), il livello piu' ricco per ultimo, cosi' graph.obj
     resta il migliore; controlli di integrita' (fermate/pattern) rispetto al build precedente dello stesso livello
  6. coerenza tra livelli: il servizio attivo di L1 deve essere contenuto in L2
Esiti (netex_monitor\\risultati): last_run.json, history.csv, netex_monitor.log, build_log\\<REGIONE>\\build_<REGIONE>_L<n>_<data>.log;
stato interno in netex_monitor\\interno\\state.json

Uso:
  python check_netex.py                        # controllo normale
  python check_netex.py --dry-run              # solo controllo, nessun download/build
  python check_netex.py --force                # rianalizza e ricostruisce tutti i livelli
  python check_netex.py --set-baseline [Campania]
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.request

sys.dont_write_bytecode = True  # niente __pycache__ nella cartella
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import netex_operators  # analisi per operatore dello snapshot (netex_operators.py)

# Struttura (dal 2026-11-17):
#   OPENTRIPPLANNER_2_8\                    jar OTP; le cartelle regione sono in NETEX_REGIONI\<REGIONE>
#   OPENTRIPPLANNER_2_8\netex_monitor\       config.json, baseline.json, .bat
#       script\     questo script e i moduli    risultati\  last_run.json, history.csv, log, build_log\<REGIONE>
#       interno\    state.json                  archivio\   backup
HERE = os.path.dirname(os.path.abspath(__file__))       # netex_monitor\script
MON = os.path.dirname(HERE)                            # netex_monitor
OTP_ROOT = os.path.dirname(MON)                        # OPENTRIPPLANNER_2_8
CONFIG = os.path.join(MON, "config.json")
BASELINE = os.path.join(MON, "baseline.json")
STATE = os.path.join(MON, "interno", "state.json")
RESULTS = os.path.join(MON, "risultati")
LAST_RUN = os.path.join(RESULTS, "last_run.json")
LOG = os.path.join(RESULTS, "netex_monitor.log")
HISTORY = os.path.join(RESULTS, "history.csv")
BUILD_LOG_DIR = os.path.join(RESULTS, "build_log")
for _d in (os.path.dirname(STATE), RESULTS, BUILD_LOG_DIR):
    os.makedirs(_d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("netex")




def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)



def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)



def fetch_snapshots(url):
    req = urllib.request.Request(url, headers={"User-Agent": "netex-monitor/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)



def _download_once(url, tmp, tag, stall_s, max_s, progress_mb):
    """Un singolo tentativo. Logga tempo di risposta, Content-Length e avanzamento.
    stall_s: secondi massimi senza ricevere byte (timeout del socket);
    max_s: durata massima complessiva del tentativo."""
    req = urllib.request.Request(url, headers={"User-Agent": "netex-monitor/1.1"})
    t0 = time.monotonic()
    log.info("[%s] connessione in corso...", tag)
    with urllib.request.urlopen(req, timeout=stall_s) as r, open(tmp, "wb") as f:
        cl = r.headers.get("Content-Length")
        total = int(cl) if cl and cl.isdigit() else None
        log.info("[%s] risposta HTTP %s dopo %.1f s | Content-Length=%s | Content-Type=%s",
                 tag, getattr(r, "status", "?"), time.monotonic() - t0,
                 f"{total / 1e6:.1f} MB" if total else "assente", r.headers.get("Content-Type"))
        h = hashlib.sha256()
        got = 0
        next_log = progress_mb * 1e6
        while True:
            chunk = r.read1(256 * 1024)  # ritorna appena arrivano dati; blocca al massimo stall_s
            if not chunk:
                break
            f.write(chunk)
            h.update(chunk)
            got += len(chunk)
            el = time.monotonic() - t0
            if got >= next_log:
                log.info("[%s] scaricati %.1f%s MB in %.0f s (%.2f MB/s)", tag, got / 1e6,
                         f" / {total / 1e6:.1f}" if total else "", el, got / 1e6 / max(el, 0.001))
                next_log += progress_mb * 1e6
            if el > max_s:
                raise TimeoutError(f"download oltre il limite di {max_s} s ({got / 1e6:.1f} MB ricevuti)")
    el = time.monotonic() - t0
    if total is not None and got != total:
        raise IOError(f"download incompleto: ricevuti {got} byte su {total}")
    log.info("[%s] trasferimento chiuso: %.1f MB in %.0f s", tag, got / 1e6, el)
    return h.hexdigest()



def download(url, dest, expected_sha256=None, cfg=None, tag=""):
    cfg = cfg or {}
    stall_s = cfg.get("download_stall_s", 90)
    max_s = cfg.get("download_max_s", 1800)
    retries = cfg.get("download_retries", 3)
    progress_mb = cfg.get("download_progress_mb", 5)
    tmp = dest + ".part"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            log.info("[%s] tentativo di download %d/%d (stallo max %d s, durata max %d s)",
                     tag, attempt, retries, stall_s, max_s)
            digest = _download_once(url, tmp, tag, stall_s, max_s, progress_mb)
            if expected_sha256 and digest.lower() != expected_sha256.lower():
                raise RuntimeError(f"SHA-256 non corrispondente: atteso {expected_sha256}, ottenuto {digest}")
            os.replace(tmp, dest)
            return digest
        except Exception as e:  # noqa
            last_err = e
            size = os.path.getsize(tmp) / 1e6 if os.path.exists(tmp) else 0
            log.warning("[%s] tentativo %d fallito dopo %.1f MB: %s: %s", tag, attempt, size, type(e).__name__, e)
            if os.path.exists(tmp):
                os.remove(tmp)
            if attempt < retries:
                wait = 30 * attempt
                log.info("[%s] nuovo tentativo tra %d s", tag, wait)
                time.sleep(wait)
    raise RuntimeError(f"download fallito dopo {retries} tentativi: {type(last_err).__name__}: {last_err}")



def update_build_config(folder, feed_id, new_source):
    """Sostituisce il campo source del feed netex con feedId indicato (o del primo feed netex)."""
    path = os.path.join(folder, "build-config.json")
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    # Il file puo' contenere chiavi duplicate: lo modifichiamo come testo per non riscriverlo.
    feeds_m = re.search(r'"transitFeeds"\s*:\s*\[(.*?)\]', raw, re.S)
    if not feeds_m:
        raise RuntimeError("transitFeeds non trovato in build-config.json")
    block = feeds_m.group(1)
    # cerca l'oggetto con il feedId giusto, altrimenti il primo "source"
    obj_pat = re.compile(r'\{[^{}]*\}', re.S)
    replaced = False
    def repl(m):
        nonlocal replaced
        obj = m.group(0)
        if replaced:
            return obj
        if feed_id and f'"{feed_id}"' not in obj:
            return obj
        new_obj, n = re.subn(r'("source"\s*:\s*")[^"]*(")', lambda mm: mm.group(1) + new_source + mm.group(2), obj, count=1)
        if n:
            replaced = True
        return new_obj
    new_block = obj_pat.sub(repl, block)
    if not replaced:
        raise RuntimeError(f"feed netex con feedId={feed_id} non trovato in build-config.json")
    new_raw = raw[:feeds_m.start(1)] + new_block + raw[feeds_m.end(1):]
    backup = path + ".bak"
    with open(backup, "w", encoding="utf-8") as f:
        f.write(raw)
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_raw)
    log.info("build-config.json aggiornato: source = %s (backup in %s)", new_source, os.path.basename(backup))



def run_build(cfg, folder_name, build_log_path):
    cmd = ["java", f"-Xmx{cfg.get('build_xmx_gb', 20)}G", "-jar", cfg["otp_jar"], "--loadStreet", "--save", folder_name]
    log.info("Avvio build: %s (cwd=%s)", " ".join(cmd), OTP_ROOT)
    started = dt.datetime.now()
    with open(build_log_path, "w", encoding="utf-8", errors="replace") as out:
        proc = subprocess.run(cmd, cwd=OTP_ROOT, stdout=out, stderr=subprocess.STDOUT,
                              timeout=cfg.get("build_timeout_s", 3 * 3600))
    elapsed = (dt.datetime.now() - started).total_seconds()
    return proc.returncode, elapsed, started



STAT_PATTERNS = {
    # riga finale del GraphBuilder: "Transit built. |Stops|=5,759 |Patterns|=1,809 |ConstrainedTransfers|=0"
    "stops": r"Transit built\.\s*\|Stops\|=([\d,.]+)",
    "patterns": r"Transit built\..*?\|Patterns\|=([\d,.]+)",
    "constrained_transfers": r"Transit built\..*?\|ConstrainedTransfers\|=([\d,.]+)",
    "vertices": r"Graph built\.\s*\|V\|=([\d,.]+)",
    "edges": r"Graph built\..*?\|E\|=([\d,.]+)",
    # "873 of 5636 TripPattern(s) removed. Reason: No trips for pattern exist."
    "patterns_removed": r"(\d[\d,.]*) of [\d,.]+ TripPattern\(s\) removed",
    "patterns_total_in_feed": r"[\d,.]+ of (\d[\d,.]*) TripPattern\(s\) removed",
    # "126 stops remain isolated"
    "isolated_stops": r"(\d[\d,.]*) stops remain isolated",
    # "Created a total of 332979 transfers from 17685 stops."
    "transfers": r"Created a total of (\d[\d,.]*) transfers",
}



def parse_build_stats(build_log_path):
    """Estrae dal log del build le statistiche finali del grafo OTP (fermate, pattern, ...).
    NB: la riga 'Transit loaded' si riferisce allo streetGraph caricato all'inizio (0 fermate):
    il dato buono e' 'Transit built' scritto da GraphBuilder alla fine."""
    try:
        with open(build_log_path, encoding="utf-8", errors="replace") as f:
            txt = f.read()
    except OSError:
        return {}
    stats = {}
    for key, pat in STAT_PATTERNS.items():
        m = re.search(pat, txt)
        if m:
            stats[key] = int(re.sub(r"[,.]", "", m.group(1)))
    return stats



def check_build_stats(stats, prev_stats, cfg):
    """Confronta le statistiche con il build precedente. Ritorna (lista avvisi, dict delta)."""
    warnings, delta = [], {}
    if not stats or "stops" not in stats:
        warnings.append("riga 'Transit built' non trovata nel log del build: impossibile verificare fermate/pattern")
        return warnings, delta
    if stats.get("stops", 0) == 0:
        warnings.append("grafo costruito con 0 fermate")
    if stats.get("patterns", 0) == 0:
        warnings.append("grafo costruito con 0 pattern")
    drop_pct = cfg.get("stats_drop_warn_pct", 20)
    for key in ("stops", "patterns"):
        old, new = (prev_stats or {}).get(key), stats.get(key)
        if old and new is not None:
            change = (new - old) / old * 100
            delta[key] = {"previous": old, "current": new, "change_pct": round(change, 1)}
            if change <= -drop_pct:
                warnings.append(f"{key}: calo del {abs(change):.1f}% rispetto al build precedente ({old:,} -> {new:,})")
    tot = stats.get("patterns_total_in_feed")
    rem = stats.get("patterns_removed")
    if tot and rem is not None and rem / tot * 100 >= cfg.get("patterns_removed_warn_pct", 50):
        warnings.append(f"{rem:,} pattern su {tot:,} scartati da OTP ({rem / tot * 100:.0f}%)")
    return warnings, delta



def summarize_report(folder):
    """Ritorna un elenco (nome_report -> numero righe tabella) dai file HTML del data-import report OTP."""
    rep = os.path.join(folder, "report")
    out = {}
    if not os.path.isdir(rep):
        return out
    for fn in sorted(os.listdir(rep)):
        if fn.endswith(".html") and fn != "index.html":
            try:
                with open(os.path.join(rep, fn), encoding="utf-8", errors="replace") as f:
                    out[re.sub(r"\d+\.html$|\.html$", "", fn)] = out.get(re.sub(r"\d+\.html$|\.html$", "", fn), 0) + f.read().count("<tr")
            except OSError:
                pass
    return out


# ---------------------------------------------------------------------------------------------
# Logica a due livelli (dal 2026-11-17)
# ---------------------------------------------------------------------------------------------

def region_levels(region):
    base = region.get("snapshot_base") or region["snapshot_id"].rsplit(".LVL-", 1)[0]
    return base, region.get("levels", [1, 2])


def level_sid(base, lvl):
    return f"{base}.LVL-{lvl}"


def analysis_params(cfg):
    return {"now_days": cfg.get("active_now_days", 7), "horizon_start": cfg.get("active_horizon_start_days", 14),
            "horizon_days": cfg.get("active_horizon_days", 7), "aliases": cfg.get("operator_aliases", {})}


def feed_summary(an):
    t = an["totals"]
    return {"ref_date": an["ref_date"], "windows": an["windows"], "lines": t["Line"], "stop_places": t["StopPlace"],
            "service_journeys": t["ServiceJourney"], "sj_active_now": t["sj_active_now"],
            "sj_active_horizon": t["sj_active_horizon"], "sj_expired": t["sj_expired"], "sj_expired_pct": t["sj_expired_pct"],
            "operators_with_lines": sum(1 for o in an["operators"].values() if o["lines"])}


def check_expiry(an, cfg):
    """Avvisi 'scadenze': quota di corse gia' scadute e operatori con orari in scadenza prima della finestra a 2-3 settimane."""
    w = []
    t = an["totals"]
    if t["ServiceJourney"] and t["sj_expired_pct"] >= cfg.get("expired_warn_pct", 20):
        w.append(f"orari scaduti: {t['sj_expired']:,} corse su {t['ServiceJourney']:,} ({t['sj_expired_pct']:.0f}%) non hanno piu' giorni di servizio")
    pct, min_sj = cfg.get("expiring_warn_pct", 50), cfg.get("expiring_min_sj", 50)
    h0, h1 = an["windows"]["horizon"]
    for o in an["operators"].values():
        n, h = o["sj_active_now"], o["sj_active_horizon"]
        if o["lines"] and o["service_journeys"] >= min_sj and n == 0 and h == 0:
            w.append(f"operatore senza servizio attivo: {o['name']} ({o['lines']} linee, {o['service_journeys']:,} corse, "
                     f"{o['sj_expired']:,} scadute, nessuna attiva nei prossimi giorni ne' tra {h0} e {h1})")
        elif n >= min_sj and h < n * (1 - pct / 100):
            w.append(f"orari in scadenza: {o['name']} ha {n:,} corse attive nei prossimi giorni ma {h:,} tra {h0} e {h1}")
    return w


def baseline_step(base, region_name, analysed, baselines, cfg, prev_statuses):
    """Baseline per regione sulle corse attive a 2-3 settimane (sj_active_horizon), condivisa tra i livelli.
    analysed: {lvl: (an, fname)} delle analisi fatte in questo run. Ritorna (avvisi per livello, delta per livello, note, aggiornata)."""
    warn = {l: [] for l in analysed}
    delta, notes, updated = {}, [], False
    pct = cfg.get("baseline_warn_pct", 15)
    if not analysed:
        return warn, delta, notes, updated
    best_lvl = max(analysed, key=lambda l: analysed[l][0]["totals"]["sj_active_horizon"])
    best_val = analysed[best_lvl][0]["totals"]["sj_active_horizon"]
    b = baselines.get(base)
    now = dt.datetime.now().isoformat(timespec="seconds")
    if not b or not b.get("value"):
        baselines[base] = {"region": region_name, "metric": "sj_active_horizon", "value": best_val, "level": f"LVL-{best_lvl}",
                           "file": analysed[best_lvl][1], "set_at": now, "set_by": "auto (prima analisi)"}
        notes.append(f"baseline impostata automaticamente: {best_val:,} corse attive a 2-3 settimane (LVL-{best_lvl})")
        updated = True
    elif best_val >= b["value"] * (1 + pct / 100):
        old = b["value"]
        baselines[base].update({"value": best_val, "level": f"LVL-{best_lvl}", "file": analysed[best_lvl][1],
                                "set_at": now, "set_by": "auto (crescita oltre soglia)"})
        warn[best_lvl].append(f"contenuto: NeTEX cresciuto oltre la baseline (corse attive a 2-3 settimane {old:,} -> {best_val:,}, "
                              f"+{(best_val - old) / old * 100:.0f}%); baseline aggiornata verso l'alto")
        updated = True
    bv = baselines[base]["value"]
    for l, (an, _f) in analysed.items():
        v = an["totals"]["sj_active_horizon"]
        ch = round((v - bv) / bv * 100, 1) if bv else None
        delta[l] = {"baseline": bv, "current": v, "change_pct": ch}
        if ch is not None and ch <= -pct:
            warn[l].append(f"contenuto: {v:,} corse attive a 2-3 settimane vs baseline {bv:,} ({ch:+.0f}%)")
        elif prev_statuses.get(l) == "build_warning" and not warn[l]:
            notes.append(f"LVL-{l}: valori rientrati entro la soglia della baseline")
    return warn, delta, notes, updated


HISTORY_FIELDS = ["data", "regione", "livello", "snapshot_id", "file", "status", "corse", "corse_attive_7gg",
                  "corse_attive_2_3sett", "corse_scadute_pct", "operatori_con_linee", "baseline_corse_attive",
                  "attive_vs_baseline_pct", "stops", "patterns", "patterns_removed", "patterns_total_in_feed",
                  "isolated_stops", "transfers", "vertices", "edges", "stops_vs_prev_pct", "patterns_vs_prev_pct"]


def migrate_history():
    """Converte history.csv al formato a due livelli (una volta sola)."""
    if not os.path.exists(HISTORY):
        return
    with open(HISTORY, encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    if rows and "livello" in rows[0]:
        return
    if not rows and open(HISTORY, encoding="utf-8").readline().startswith("data;regione;livello"):
        return
    os.replace(HISTORY, HISTORY + ".v1.bak")
    with open(HISTORY, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_FIELDS, delimiter=";", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            r["livello"] = r.get("snapshot_id", "").rsplit("-", 1)[-1]
            w.writerow(r)


def append_history_v2(region_name, lvl, sid, fname, status, fs, b_delta, stats, prev_delta):
    row = {"data": dt.datetime.now().isoformat(timespec="seconds"), "regione": region_name, "livello": lvl,
           "snapshot_id": sid, "file": fname, "status": status}
    if fs:
        row.update({"corse": fs["service_journeys"], "corse_attive_7gg": fs["sj_active_now"],
                    "corse_attive_2_3sett": fs["sj_active_horizon"], "corse_scadute_pct": fs["sj_expired_pct"],
                    "operatori_con_linee": fs["operators_with_lines"]})
    if b_delta:
        row.update({"baseline_corse_attive": b_delta.get("baseline"), "attive_vs_baseline_pct": b_delta.get("change_pct")})
    for k in ("stops", "patterns", "patterns_removed", "patterns_total_in_feed", "isolated_stops", "transfers", "vertices", "edges"):
        row[k] = (stats or {}).get(k, "")
    row["stops_vs_prev_pct"] = ((prev_delta or {}).get("stops") or {}).get("change_pct", "")
    row["patterns_vs_prev_pct"] = ((prev_delta or {}).get("patterns") or {}).get("change_pct", "")
    new = not os.path.exists(HISTORY)
    with open(HISTORY, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_FIELDS, delimiter=";", extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow(row)


def cleanup_zips(folder, lvl, keep, protect):
    zips = sorted([f for f in os.listdir(folder) if f.endswith(".xml.zip") and f"_L{lvl}_" in f],
                  key=lambda f: os.path.getmtime(os.path.join(folder, f)))
    for old in (zips[:-keep] if keep else []):
        if old not in protect:
            try:
                os.remove(os.path.join(folder, old))
                log.info("rimosso vecchio snapshot %s", old)
            except OSError as e:
                log.warning("impossibile rimuovere %s: %s", old, e)


def process_region(cfg, region, snapshots, state, args, run_date):
    name = region.get("name")
    base, levels = region_levels(region)
    folder_name = region["folder"]
    folder = os.path.join(OTP_ROOT, folder_name)
    params = analysis_params(cfg)
    build_enabled = cfg.get("build_enabled", True)   # false = solo analisi del feed (es. GitHub Actions)
    os.makedirs(folder, exist_ok=True)
    by_id = {s.get("snapshotId"): s for s in snapshots}
    results, todo, analysed = {}, [], {}

    # --- fase 1: per ogni livello controllo novita', download e analisi del feed (nessun build)
    for lvl in levels:
        sid = level_sid(base, lvl)
        res = {"level": lvl, "snapshot_id": sid, "status": "no_update"}
        results[lvl] = res
        snap = by_id.get(sid)
        if snap is None:
            res["status"] = "not_available"
            log.info("[%s] snapshot non presente nell'API", sid)
            continue
        mod_time = snap.get("modificationTime")
        dl = (snap.get("downloadData") or [{}])[0]
        link, sha = dl.get("downloadLink"), dl.get("integrityHash")
        fname = link.rsplit("/", 1)[-1] if link else None
        res.update({"remote_modification_time": mod_time, "remote_file": fname})
        prev = state.get(sid, {})
        log.info("[%s] remoto: %s (%s) | noto: %s", sid, mod_time, fname, prev.get("modification_time"))
        is_new = (mod_time != prev.get("modification_time")) or (build_enabled and not os.path.exists(os.path.join(folder, fname or "")))
        if not is_new and not args.force:
            res.update({"current_file": prev.get("file"), "feed": prev.get("feed"), "transit_stats": prev.get("transit_stats")})
            continue
        if args.dry_run:
            res["status"] = "update_available"
            continue
        try:
            dest = os.path.join(folder, fname)
            if not os.path.exists(dest):
                log.info("[%s] download %s -> %s", sid, link, dest)
                download(link, dest, sha, cfg, sid)
                log.info("[%s] download completato (%.1f MB), hash verificato", sid, os.path.getsize(dest) / 1e6)
            else:
                log.info("[%s] file gia' presente, salto il download", sid)
        except Exception as e:  # noqa
            res.update({"status": "error", "error": f"download: {e}"})
            log.exception("[%s] download fallito: %s", sid, e)
            continue
        warnings, an, op_rows = [], None, []
        try:
            an = netex_operators.analyze(dest, ref_date=run_date, **params)
            fs = feed_summary(an)
            log.info("[%s] analisi feed: %d operatori con linee, %s linee, %s corse (%s attive 7gg, %s attive %s..%s, %s%% scadute) in %d s",
                     sid, fs["operators_with_lines"], f"{fs['lines']:,}", f"{fs['service_journeys']:,}", f"{fs['sj_active_now']:,}",
                     f"{fs['sj_active_horizon']:,}", an["windows"]["horizon"][0], an["windows"]["horizon"][1], fs["sj_expired_pct"], an["seconds"])
            if prev.get("operator_stats"):
                ow, op_rows = netex_operators.compare(prev["operator_stats"], an, cfg.get("operator_sj_drop_warn_pct", 40))
                warnings += ow
            warnings += check_expiry(an, cfg)
            res["feed"] = fs
            res["operators_attention"] = {r["operatore"]: {k: r[k] for k in ("lines_prev", "lines_cur", "sj_prev", "sj_cur", "sj_change_pct", "stato")}
                                          for r in op_rows if r["stato"] in ("SPARITO", "NUOVO", "CALO CORSE")}
            analysed[lvl] = (an, fname)
        except Exception as e:  # noqa
            log.exception("[%s] analisi feed fallita: %s", sid, e)
            warnings.append(f"analisi feed fallita: {e}")
        for w in warnings:
            log.warning("[%s] %s", sid, w)
        todo.append({"lvl": lvl, "sid": sid, "fname": fname, "mod_time": mod_time, "sha": sha, "prev": prev,
                     "warnings": warnings, "an": an})

    # --- fase 2: baseline di regione (corse attive a 2-3 settimane), condivisa tra i livelli
    baselines = load_json(BASELINE, {})
    b_warn, b_delta, b_notes, b_updated = baseline_step(base, name, analysed, baselines, cfg,
                                                        {l: state.get(level_sid(base, l), {}).get("last_status") for l in levels})
    if b_updated:
        save_json(BASELINE, baselines)
    for n in b_notes:
        log.info("[%s] %s", base, n)
    for l, ws in b_warn.items():
        for w in ws:
            log.warning("[%s] %s", level_sid(base, l), w)

    # --- fase 3: build di ogni livello nuovo; il piu' ricco per ultimo, cosi' graph.obj resta il migliore
    if not build_enabled:
        for item in todo:
            lvl, sid, fname, prev, res = item["lvl"], item["sid"], item["fname"], item["prev"], results[item["lvl"]]
            warnings = item["warnings"] + b_warn.get(lvl, [])
            res.update({"status": "feed_warning" if warnings else "feed_ok", "baseline_delta": b_delta.get(lvl), "warnings": warnings})
            append_history_v2(name, lvl, sid, fname, res["status"], res.get("feed"), b_delta.get(lvl), None, None)
            state[sid] = {"modification_time": item["mod_time"], "file": fname, "sha256": item["sha"],
                          "last_status": res["status"], "checked_at": dt.datetime.now().isoformat(timespec="seconds"),
                          "transit_stats": prev.get("transit_stats"), "feed": res.get("feed") or prev.get("feed"),
                          "operator_stats": netex_operators.compact(item["an"]) if item["an"] else prev.get("operator_stats")}
            save_json(STATE, state)
        todo_build = []
    else:
        todo_build = todo
    graph = os.path.join(folder, "graph.obj")
    for item in sorted(todo_build, key=lambda i: (i["an"] or {"totals": {"sj_active_horizon": -1}})["totals"]["sj_active_horizon"]):
        lvl, sid, fname, prev, res = item["lvl"], item["sid"], item["fname"], item["prev"], results[item["lvl"]]
        warnings = item["warnings"] + b_warn.get(lvl, [])
        try:
            update_build_config(folder, region.get("feed_id"), fname)
            before = os.path.getmtime(graph) if os.path.exists(graph) else 0
            reg_tag = re.sub(r"[^A-Za-z0-9]+", "_", name or folder_name).upper()
            os.makedirs(os.path.join(BUILD_LOG_DIR, reg_tag), exist_ok=True)
            build_log = os.path.join(BUILD_LOG_DIR, reg_tag, f"build_{reg_tag}_L{lvl}_{dt.datetime.now():%Y%m%d_%H%M%S}.log")
            rc, elapsed, _ = run_build(cfg, folder_name, build_log)
            graph_ok = os.path.exists(graph) and os.path.getmtime(graph) > before
            stats = parse_build_stats(build_log)
            iw, tdelta = check_build_stats(stats, prev.get("transit_stats"), cfg)
            warnings += iw
            res.update({"build_returncode": rc, "build_seconds": round(elapsed), "build_log": os.path.relpath(build_log, MON).replace("\\", "/"),
                        "graph_regenerated": graph_ok, "transit_stats": stats, "transit_delta": tdelta,
                        "baseline_delta": b_delta.get(lvl), "warnings": warnings, "report_summary": summarize_report(folder)})
            if rc == 0 and graph_ok:
                res["status"] = "build_warning" if warnings else "build_ok"
                (log.warning if warnings else log.info)("[%s] BUILD %s in %.0f s | fermate=%s pattern=%s", sid,
                                                        "COMPLETATO CON AVVISI" if warnings else "OK", elapsed,
                                                        f"{stats.get('stops', 0):,}", f"{stats.get('patterns', 0):,}")
                state.setdefault(base, {})["graph"] = {"level": lvl, "file": fname, "built_at": dt.datetime.now().isoformat(timespec="seconds")}
            else:
                res["status"] = "build_failed"
                log.error("[%s] BUILD FALLITO (rc=%s, graph rigenerato=%s). Vedi %s", sid, rc, graph_ok, build_log)
                bak = os.path.join(folder, "build-config.json.bak")
                if os.path.exists(bak):
                    os.replace(bak, os.path.join(folder, "build-config.json"))
                    log.info("[%s] build-config.json ripristinato dal backup", sid)
            if graph_ok:
                append_history_v2(name, lvl, sid, fname, res["status"], res.get("feed"), b_delta.get(lvl), stats, tdelta)
            state[sid] = {"modification_time": item["mod_time"], "file": fname, "sha256": item["sha"],
                          "last_status": res["status"], "checked_at": dt.datetime.now().isoformat(timespec="seconds"),
                          "transit_stats": stats if graph_ok else prev.get("transit_stats"),
                          "feed": res.get("feed") or prev.get("feed"),
                          "operator_stats": netex_operators.compact(item["an"]) if item["an"] else prev.get("operator_stats")}
            save_json(STATE, state)
        except Exception as e:  # noqa
            res.update({"status": "error", "error": str(e), "warnings": warnings})
            log.exception("[%s] errore: %s", sid, e)

    # --- fase 4: coerenza tra livelli (servizio L1 contenuto in L2), alla stessa data di riferimento
    consistency = []
    if 1 in levels and 2 in levels and analysed:
        ans = {}
        for l in (1, 2):
            if l in analysed:
                ans[l] = analysed[l][0]
            else:
                f = (state.get(level_sid(base, l)) or {}).get("file")
                p = os.path.join(folder, f) if f else None
                snap_l = by_id.get(level_sid(base, l))
                if p and not os.path.exists(p) and snap_l:
                    dl = (snap_l.get("downloadData") or [{}])[0]
                    if dl.get("downloadLink", "").endswith(f):
                        try:
                            log.info("[%s] scarico LVL-%d per il controllo di coerenza", base, l)
                            download(dl["downloadLink"], p, dl.get("integrityHash"), cfg, level_sid(base, l))
                        except Exception as e:  # noqa
                            log.warning("[%s] download LVL-%d per la coerenza fallito: %s", base, l, e)
                if p and os.path.exists(p):
                    try:
                        ans[l] = netex_operators.analyze(p, ref_date=run_date, **params)
                    except Exception as e:  # noqa
                        log.warning("[%s] rianalisi LVL-%d per la coerenza fallita: %s", base, l, e)
        if 1 in ans and 2 in ans:
            consistency = netex_operators.level_consistency(ans[1], ans[2], cfg.get("level_consistency_tol_pct", 10))
            for w in consistency:
                log.warning("[%s] %s", base, w)

    protect = {(state.get(level_sid(base, l)) or {}).get("file") for l in levels}
    for l in levels:
        cleanup_zips(folder, l, cfg.get("keep_old_zips", 3), protect)
    return {"region": name, "base": base, "folder": folder_name, "graph": (state.get(base) or {}).get("graph"),
            "baseline": baselines.get(base), "notes": b_notes, "consistency_warnings": consistency,
            "levels": [results[l] for l in levels]}


def set_baseline_v2(cfg, state, which, run_date):
    baselines = load_json(BASELINE, {})
    params = analysis_params(cfg)
    for region in cfg["regions"]:
        base, levels = region_levels(region)
        if which not in ("all", base, region.get("name")):
            continue
        best = None
        for l in levels:
            f = (state.get(level_sid(base, l)) or {}).get("file")
            p = os.path.join(OTP_ROOT, region["folder"], f) if f else None
            if p and os.path.exists(p):
                v = netex_operators.analyze(p, ref_date=run_date, **params)["totals"]["sj_active_horizon"]
                if best is None or v > best[0]:
                    best = (v, l, f)
        if best is None:
            log.warning("[%s] nessun file analizzabile: baseline non impostata", base)
            continue
        baselines[base] = {"region": region.get("name"), "metric": "sj_active_horizon", "value": best[0], "level": f"LVL-{best[1]}",
                           "file": best[2], "set_at": dt.datetime.now().isoformat(timespec="seconds"), "set_by": "manuale (--set-baseline)"}
        log.info("[%s] baseline impostata: %s corse attive a 2-3 settimane (LVL-%d, %s)", base, f"{best[0]:,}", best[1], best[2])
    save_json(BASELINE, baselines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="solo controllo, nessun download/build")
    ap.add_argument("--force", action="store_true", help="rianalizza e ricostruisce tutti i livelli anche senza novita'")
    ap.add_argument("--set-baseline", nargs="?", const="all", metavar="REGIONE",
                    help="imposta la baseline (corse attive a 2-3 settimane) dai file in uso, per tutte le regioni o per quella indicata")
    args = ap.parse_args()
    cfg = load_json(CONFIG, None)
    if cfg is None:
        log.error("config.json mancante in %s", MON)
        sys.exit(2)
    global OTP_ROOT
    if cfg.get("otp_root"):   # es. "work" su GitHub: cartelle regione relative a netex_monitor/work
        OTP_ROOT = os.path.abspath(os.path.join(MON, cfg["otp_root"]))
        os.makedirs(OTP_ROOT, exist_ok=True)
    state = load_json(STATE, {})
    run_date = dt.date.today()
    if args.set_baseline:
        set_baseline_v2(cfg, state, args.set_baseline, run_date)
        return
    migrate_history()
    run = {"run_at": dt.datetime.now().isoformat(timespec="seconds"), "dry_run": args.dry_run, "regions": []}
    log.info("=== avvio controllo (dry_run=%s, force=%s) ===", args.dry_run, args.force)
    try:
        snapshots = fetch_snapshots(cfg["api_url"])
        log.info("API ok: %d snapshot", len(snapshots))
    except Exception as e:  # noqa
        log.exception("Impossibile interrogare l'API: %s", e)
        run["error"] = f"API non raggiungibile: {e}"
        save_json(LAST_RUN, run)
        sys.exit(1)
    for region in cfg["regions"]:
        if region.get("enabled", True) is False:
            log.info("[%s] regione in pausa (enabled=false in config.json): salto", region.get("name"))
            run["regions"].append({"region": region.get("name"), "status": "paused", "note": region.get("note")})
            continue
        run["regions"].append(process_region(cfg, region, snapshots, state, args, run_date))
    save_json(STATE, state)
    save_json(LAST_RUN, run)
    statuses = [lv["status"] for r in run["regions"] for lv in r.get("levels", [])]
    log.info("=== fine: %s ===", "; ".join(
        f'{r["region"]}=' + (r["status"] if "status" in r else ",".join(f'L{lv["level"]}:{lv["status"]}' for lv in r["levels"]))
        for r in run["regions"]))
    sys.exit(1 if any(s in ("error", "build_failed") for s in statuses) else 0)  # feed_ok / feed_warning = 0


if __name__ == "__main__":
    main()

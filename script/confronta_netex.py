# -*- coding: utf-8 -*-
"""
confronta_netex.py - Confronto manuale per operatore tra due snapshot NeTEX (stesso livello o L1 vs L2).

Uso:
  python netex_monitor\\script\\confronta_netex.py <snapshot_A.zip> <snapshot_B.zip> [--soglia 40] [--linee] [--livelli] [--data AAAA-MM-GG] [--json out.json]
    A = precedente (o L1), B = corrente (o L2)
    --soglia   calo % delle corse attive a 2-3 settimane per operatore oltre il quale segnalare (default 40)
    --linee    elenca anche le linee perse/nuove per ogni operatore
    --livelli  verifica anche che il servizio di A (L1) sia contenuto in B (L2)
    --data     data di riferimento per le corse attive (default oggi)
    --json     salva il confronto completo in un file JSON
Finestre e alias degli operatori sono letti da config.json (active_*, operator_aliases).
"""
import argparse
import datetime as dt
import json
import os
import sys

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))   # netex_monitor\script
MON = os.path.dirname(HERE)                         # netex_monitor (config.json)
sys.path.insert(0, HERE)
from netex_operators import analyze, compare, format_table, level_consistency  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prev")
    ap.add_argument("cur")
    ap.add_argument("--soglia", type=float, default=40)
    ap.add_argument("--linee", action="store_true")
    ap.add_argument("--livelli", action="store_true")
    ap.add_argument("--data")
    ap.add_argument("--json")
    a = ap.parse_args()
    cfg = json.load(open(os.path.join(MON, "config.json"), encoding="utf-8")) if os.path.exists(os.path.join(MON, "config.json")) else {}
    kw = {"ref_date": dt.date.fromisoformat(a.data) if a.data else dt.date.today(), "now_days": cfg.get("active_now_days", 7),
          "horizon_start": cfg.get("active_horizon_start_days", 14), "horizon_days": cfg.get("active_horizon_days", 7),
          "aliases": cfg.get("operator_aliases", {})}
    res = {}
    for lab, path in (("A", a.prev), ("B", a.cur)):
        print(f"Analisi {lab}: {os.path.basename(path)} ...", flush=True)
        r = analyze(path, **kw)
        t = r["totals"]
        print(f"  {r['seconds']} s | linee {t['Line']:,} | StopPlace {t['StopPlace']:,} | corse {t['ServiceJourney']:,} | "
              f"attive {r['windows']['now'][0]}..{r['windows']['now'][1]}: {t['sj_active_now']:,} | "
              f"attive {r['windows']['horizon'][0]}..{r['windows']['horizon'][1]}: {t['sj_active_horizon']:,} | scadute {t['sj_expired_pct']}%")
        res[lab] = r
    warnings, rows = compare(res["A"], res["B"], a.soglia)
    print("\nCorse attive a 2-3 settimane per operatore (A -> B):\n")
    print(format_table(rows, "A", "B"))
    if a.linee:
        for r in rows:
            lp = set(res["A"]["operators"].get(r["key"], {}).get("line_names", []))
            lc = set(res["B"]["operators"].get(r["key"], {}).get("line_names", []))
            if lp - lc or lc - lp:
                print(f"\n{r['operatore']}: linee perse {sorted(lp - lc)} | nuove {sorted(lc - lp)}")
    if a.livelli:
        warnings += level_consistency(res["A"], res["B"], cfg.get("level_consistency_tol_pct", 10))
    print("\nAVVISI:" if warnings else f"\nNessun avviso (soglia calo corse {a.soglia:.0f}%).")
    for w in warnings:
        print(" -", w)
    if a.json:
        json.dump({"A": res["A"], "B": res["B"], "warnings": warnings, "rows": rows}, open(a.json, "w", encoding="utf-8"), indent=1, ensure_ascii=False)
        print("salvato", a.json)


if __name__ == "__main__":
    main()

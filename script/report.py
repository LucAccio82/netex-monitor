# -*- coding: utf-8 -*-
"""
report.py - Genera risultati/REPORT.md e i grafici risultati/grafici/<REGIONE>.png
a partire da history.csv, last_run.json, baseline.json e interno/state.json.

Uso:  python script/report.py
Richiede matplotlib solo per i grafici (senza matplotlib il report viene generato senza immagini).
"""
import csv
import datetime as dt
import json
import os
import re
import sys

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))
MON = os.path.dirname(HERE)
RESULTS = os.path.join(MON, "risultati")
CHART_DIR = os.path.join(RESULTS, "grafici")
REPORT = os.path.join(RESULTS, "REPORT.md")

# palette (dataviz, modo chiaro): serie in ordine fisso, testo e assi neutri
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
LEVEL_COLOR = {"1": "#2a78d6", "2": "#eb6834"}   # slot 1 blu = L1, slot 2 arancio = L2
STATUS_LABEL = {"no_update": "nessuna novità", "update_available": "novità disponibile", "not_available": "non pubblicato",
                "feed_ok": "ok", "feed_warning": "⚠️ avvisi", "build_ok": "build ok", "build_warning": "⚠️ build con avvisi",
                "build_failed": "❌ build fallito", "error": "❌ errore", "paused": "in pausa"}


def load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def read_history(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        head = f.readline()
    delim = ";" if head.count(";") > head.count(",") else ","
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=delim))


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def snap_date(fname):
    m = re.search(r"_(\d{4})_(\d{2})_(\d{2})_\d{2}_\d{2}_\d{2}", fname or "")
    return f"{m.group(3)}/{m.group(2)}/{m.group(1)}" if m else "-"


def fmt(n):
    return f"{int(n):,}".replace(",", ".") if n is not None else "-"


def pct(n):
    return f"{n:+.0f}%" if n is not None else "-"


def chart(region, rows, baseline_now):
    """Una serie per livello (corse attive a 2-3 settimane) + baseline in grigio tratteggiato."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    pts = {}
    base_pts = []
    for r in rows:
        v = num(r.get("corse_attive_2_3sett"))
        if v is None:
            continue
        t = dt.datetime.fromisoformat(r["data"])
        pts.setdefault(str(r.get("livello")), []).append((t, v))
        b = num(r.get("baseline_corse_attive"))
        if b is not None:
            base_pts.append((t, b))
    if not pts:
        return None
    fig, ax = plt.subplots(figsize=(8.6, 3.6), dpi=130)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    all_t = sorted(t for s in pts.values() for t, _ in s)
    if len(all_t) == 1 or all_t[0] == all_t[-1]:
        x0, x1 = all_t[0] - dt.timedelta(days=3), all_t[-1] + dt.timedelta(days=4)
    else:
        span = all_t[-1] - all_t[0]
        x0, x1 = all_t[0] - span * 0.03, all_t[-1] + span * 0.15
    if base_pts:
        base_pts.sort()
        bt = [x0] + [t for t, _ in base_pts][1:] + [x1]
        bv = [v for _, v in base_pts] + [baseline_now or base_pts[-1][1]]
        ax.step(bt, bv, where="post", color=MUTED, lw=1.5, ls=(0, (4, 3)), label="baseline", zorder=1)
    for lvl in sorted(pts):
        s = sorted(pts[lvl])
        ax.plot([t for t, _ in s], [v for _, v in s], color=LEVEL_COLOR.get(lvl, INK2), lw=2, marker="o", ms=6,
                markeredgecolor=SURFACE, markeredgewidth=1.5, label=f"livello {lvl}", zorder=3)
        t_last, v_last = s[-1]
        ax.annotate(f"L{lvl}  {fmt(v_last)}", (t_last, v_last), xytext=(8, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK2, zorder=4,
                    bbox=dict(boxstyle="round,pad=0.25", fc=SURFACE, ec="none"))
    ax.set_title(f"{region} – corse attive a 2-3 settimane", loc="left", fontsize=11, color=INK, pad=10)
    ymax = max([v for s in pts.values() for _, v in s] + [v for _, v in base_pts] + [baseline_now or 0])
    ax.set_ylim(0, ymax * 1.18 if ymax else 1)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: fmt(v)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=10))
    ax.set_xlim(x0, x1)
    ax.grid(axis="y", color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    leg = ax.legend(loc="lower right", bbox_to_anchor=(1.0, 1.0), frameon=False, fontsize=9, ncol=3, borderaxespad=0.2)
    for t in leg.get_texts():
        t.set_color(INK2)
    fig.tight_layout()
    os.makedirs(CHART_DIR, exist_ok=True)
    name = re.sub(r"[^A-Za-z0-9]+", "_", region).upper() + ".png"
    fig.savefig(os.path.join(CHART_DIR, name), facecolor=SURFACE)
    plt.close(fig)
    return f"grafici/{name}"


def main():
    cfg = load(os.path.join(MON, "config.json"), {})
    last = load(os.path.join(RESULTS, "last_run.json"), {})
    baselines = load(os.path.join(MON, "baseline.json"), {})
    state = load(os.path.join(MON, "interno", "state.json"), {})
    hist = read_history(os.path.join(RESULTS, "history.csv"))
    by_region = {r.get("region"): r for r in last.get("regions", [])}
    try:
        import matplotlib  # noqa: F401
        have_mpl = True
    except ImportError:
        have_mpl = False

    run_at = last.get("run_at", "-").replace("T", " ")
    out = ["# Report monitoraggio NeTEx", "",
           f"Ultimo controllo: **{run_at}**  ·  generato il {dt.datetime.now():%d/%m/%Y %H:%M}", "",
           "*Corse attive a 2-3 settimane* = corse con almeno un giorno di servizio tra il 14° e il 20° giorno dalla data "
           "dell'analisi. La *baseline* è il valore del feed più completo conosciuto per la regione.", ""]

    # --- tabella riassuntiva
    out += ["## Situazione per regione e livello", "",
            "| Regione | Livello | Versione | Ultimo esito | Operatori | Corse | Attive 7 gg | Attive 2-3 sett. | vs baseline | Scadute | Analisi del |",
            "|---|---|---|---|--:|--:|--:|--:|--:|--:|---|"]
    for region in cfg.get("regions", []):
        name, base = region["name"], region.get("snapshot_base")
        r = by_region.get(name, {})
        bval = (baselines.get(base) or {}).get("value")
        levels = {lv["level"]: lv for lv in r.get("levels", [])}
        for lvl in region.get("levels", [1, 2]):
            st = state.get(f"{base}.LVL-{lvl}", {})
            lv = levels.get(lvl, {})
            f = lv.get("feed") or st.get("feed") or {}
            fname = lv.get("remote_file") or lv.get("current_file") or st.get("file")
            status = lv.get("status") or r.get("status") or "-"
            act = f.get("sj_active_horizon")
            vs = (act - bval) / bval * 100 if act is not None and bval else None
            out.append(f"| {name} | L{lvl} | {snap_date(fname)} | {STATUS_LABEL.get(status, status)} | "
                       f"{f.get('operators_with_lines', '-')} | {fmt(f.get('service_journeys'))} | {fmt(f.get('sj_active_now'))} | "
                       f"{fmt(act)} | {pct(vs)} | {('%.0f%%' % f['sj_expired_pct']) if f.get('sj_expired_pct') is not None else '-'} | "
                       f"{dt.date.fromisoformat(f['ref_date']).strftime('%d/%m') if f.get('ref_date') else '-'} |")
    out += ["", "Baseline: " + ", ".join(f"{(b.get('region') or k)} **{fmt(b.get('value'))}** (L{str(b.get('level', '')).split('-')[-1]}, "
                                          f"{snap_date(b.get('file'))})" for k, b in baselines.items()), ""]

    # --- avvisi aperti (ultimi noti per livello e coerenza tra livelli)
    out += ["## Avvisi aperti", "", "Ultimi avvisi noti per ciascun livello (si aggiornano quando esce una nuova versione).", ""]
    any_w = False
    for region in cfg.get("regions", []):
        name, base = region["name"], region.get("snapshot_base")
        block = []
        for lvl in region.get("levels", [1, 2]):
            st = state.get(f"{base}.LVL-{lvl}", {})
            ws = st.get("warnings") or []
            if ws:
                block.append(f"**Livello {lvl}** (versione del {snap_date(st.get('file'))}, analisi del {st.get('warnings_date', '-')}):")
                block += [f"- {w}" for w in ws] + [""]
        cons = (state.get(base) or {}).get("consistency") or {}
        if cons.get("warnings"):
            block.append(f"**Coerenza L1 ⊆ L2** (verifica del {cons.get('date', '-')}):")
            block += [f"- {w}" for w in cons["warnings"]] + [""]
        if block:
            any_w = True
            out += [f"### {name}", ""] + block
    if not any_w:
        out += ["Nessun avviso aperto.", ""]

    # --- grafici
    out += ["## Andamento", ""]
    if not have_mpl:
        out += ["*(matplotlib non disponibile: grafici non generati)*", ""]
    for region in cfg.get("regions", []):
        name, base = region["name"], region.get("snapshot_base")
        rows = [h for h in hist if h.get("regione") == name]
        img = chart(name, rows, (baselines.get(base) or {}).get("value")) if have_mpl else None
        if img:
            out += [f"![{name}: corse attive a 2-3 settimane per livello]({img})", ""]
        else:
            out += [f"*{name}: ancora nessun dato sulle corse attive nello storico.*", ""]
    out += ["Dati completi: [history.csv](history.csv) · esito dell'ultimo controllo: [last_run.json](last_run.json) · "
            "log: [netex_monitor.log](netex_monitor.log)", ""]
    os.makedirs(RESULTS, exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    print("report scritto:", REPORT)


if __name__ == "__main__":
    main()

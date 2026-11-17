# -*- coding: utf-8 -*-
"""
netex_operators.py - Analisi per operatore di uno snapshot NeTEX (CCISS / MaaS4Italy), livelli 1 e 2.

analyze(zip_path, ref_date=None, now_days=7, horizon_start=14, horizon_days=7, aliases=None)
  legge lo zip in streaming (senza estrarlo) e ritorna:
  {
    "file", "seconds", "ref_date", "windows": {"now": [da, a], "horizon": [da, a]},
    "totals": {"Line", "StopPlace", "ServiceJourney", "sj_active_now", "sj_active_horizon", "sj_expired", ...},
    "operators": {"<chiave operatore>": {"name", "ids", "lines", "service_journeys", "sj_active_now",
                                         "sj_active_horizon", "sj_expired", "modes", "line_names"}}
  }
  - sj_active_now:     corse con almeno un giorno di servizio nei prossimi `now_days` giorni
  - sj_active_horizon: corse con almeno un giorno di servizio tra ref_date+horizon_start e +horizon_days
                       (copertura "a 2-3 settimane": il feed copre ancora il servizio al prossimo aggiornamento?)
  - sj_expired:        corse senza alcun giorno di servizio da ref_date in poi (ValidBetween terminato o calendario finito)
  Calendario: DayType -> DayTypeAssignment -> UicOperatingPeriod (ValidDayBits) / OperatingPeriod (+DaysOfWeek) / Date,
  intersecato con il ValidBetween della ServiceJourney. Operatore della corsa: OperatorRef sulla corsa, altrimenti
  quello della linea (LineRef diretto, LineView/FlexibleLineView, o ServiceJourneyPattern -> LineRef/RouteRef).
  Chiave operatore = id NeTEx, eventualmente rimappato con `aliases` (id -> id canonico) per gli operatori che
  cambiano id tra livelli o versioni.

compare(prev, cur, sj_drop_warn_pct=40, prev_label, cur_label) confronta due analisi -> (avvisi, righe).
"""
import collections
import datetime as dt
import time
import xml.etree.ElementTree as ET
import zipfile

DOW = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
CLEAR_TAGS = {"Line", "FlexibleLine", "Route", "ServiceJourneyPattern", "JourneyPattern", "ServiceJourney", "StopPlace",
              "Quay", "TimetabledPassingTime", "PassengerStopAssignment", "ScheduledStopPoint", "ServiceLink",
              "DayTypeAssignment", "UicOperatingPeriod", "OperatingPeriod", "DayType", "Operator", "Authority"}


def _local(tag):
    return tag.split("}")[-1]


def _child(el, name):
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _txt(el, name):
    c = _child(el, name)
    return (c.text or "").strip() if c is not None else ""


def _ref_child(el, name):
    c = _child(el, name)
    return c.get("ref") if c is not None else None


def _ref_any(el, name):
    for c in el.iter():
        if _local(c.tag) == name:
            return c.get("ref")
    return None


def _date(s):
    try:
        return dt.date.fromisoformat(s[:10]) if s else None
    except ValueError:
        return None


def analyze(zip_path, ref_date=None, now_days=7, horizon_start=14, horizon_days=7, aliases=None):
    t0 = time.time()
    aliases = aliases or {}
    today = ref_date or dt.date.today()
    w_now = {today + dt.timedelta(days=i) for i in range(now_days)}
    w_hor = {today + dt.timedelta(days=horizon_start + i) for i in range(horizon_days)}
    z = zipfile.ZipFile(zip_path)
    ops, auths, lines = {}, {}, {}
    sjp_line, route_line = {}, {}
    daytype_dows, periods, dta, sjs = {}, {}, [], []
    stopplaces = 0
    for name in [n for n in z.namelist() if n.lower().endswith(".xml")]:
        with z.open(name) as f:
            for _ev, el in ET.iterparse(f, events=("end",)):
                t = _local(el.tag)
                if t == "Operator":
                    ops[el.get("id")] = _txt(el, "Name") or _txt(el, "ShortName") or el.get("id")
                elif t == "Authority":
                    auths[el.get("id")] = _txt(el, "Name") or _txt(el, "ShortName") or el.get("id")
                elif t in ("Line", "FlexibleLine"):
                    lines[el.get("id")] = {"op": _ref_child(el, "OperatorRef") or _ref_child(el, "AuthorityRef"),
                                           "name": _txt(el, "PublicCode") or _txt(el, "Name") or el.get("id"),
                                           "mode": _txt(el, "TransportMode")}
                elif t == "Route":
                    route_line[el.get("id")] = _ref_any(el, "LineRef")
                elif t in ("ServiceJourneyPattern", "JourneyPattern"):
                    sjp_line[el.get("id")] = _ref_any(el, "LineRef") or _ref_any(el, "RouteRef")
                elif t == "DayType":
                    dows = set()
                    for c in el.iter():
                        if _local(c.tag) == "DaysOfWeek" and c.text:
                            for w in c.text.split():
                                if w in DOW:
                                    dows.add(DOW[w])
                                elif w == "Weekdays":
                                    dows |= {0, 1, 2, 3, 4}
                                elif w == "Weekend":
                                    dows |= {5, 6}
                                elif w == "Everyday":
                                    dows |= set(range(7))
                    daytype_dows[el.get("id")] = dows or None
                elif t == "UicOperatingPeriod":
                    fd, bits = _date(_txt(el, "FromDate")), _txt(el, "ValidDayBits")
                    periods[el.get("id")] = {fd + dt.timedelta(days=i) for i, b in enumerate(bits) if b == "1"} if fd else set()
                elif t == "OperatingPeriod":
                    periods[el.get("id")] = (_date(_txt(el, "FromDate")), _date(_txt(el, "ToDate")))
                elif t == "DayTypeAssignment":
                    dta.append((_ref_child(el, "DayTypeRef"), _ref_child(el, "OperatingPeriodRef"),
                                _date(_txt(el, "Date")), _txt(el, "isAvailable").lower() != "false"))
                elif t == "ServiceJourney":
                    vb = _child(el, "ValidBetween")
                    vf, vt = (_date(_txt(vb, "FromDate")), _date(_txt(vb, "ToDate"))) if vb is not None else (None, None)
                    lr, sjp = _ref_child(el, "LineRef"), None
                    for c in el:
                        lt = _local(c.tag)
                        if lt in ("LineView", "FlexibleLineView") and lr is None:
                            lr = _ref_child(c, "LineRef")
                        elif lt in ("ServiceJourneyPatternRef", "JourneyPatternRef"):
                            sjp = c.get("ref")
                    dts = [c.get("ref") for c in el.iter() if _local(c.tag) == "DayTypeRef"]
                    sjs.append((_ref_child(el, "OperatorRef"), lr, sjp, dts, vf, vt))
                elif t == "StopPlace":
                    stopplaces += 1
                if t in CLEAR_TAGS:
                    el.clear()

    # date di servizio per DayType
    dt_dates, dt_excl, dt_assigned = collections.defaultdict(set), collections.defaultdict(set), set()
    for dtr, pr, dd, av in dta:
        if dtr is None:
            continue
        dt_assigned.add(dtr)
        if dd:
            dates = {dd}
        elif isinstance(periods.get(pr), set):
            dates = periods[pr]
        elif isinstance(periods.get(pr), tuple) and all(periods[pr]):
            fd, td = periods[pr]
            dows = daytype_dows.get(dtr)
            dates = {fd + dt.timedelta(days=i) for i in range((td - fd).days + 1)
                     if dows is None or (fd + dt.timedelta(days=i)).weekday() in dows}
        else:
            dates = set()
        (dt_dates if av else dt_excl)[dtr] |= dates
    dt_final = {k: dt_dates[k] - dt_excl[k] for k in dt_assigned}
    dt_max = {k: (max(v) if v else None) for k, v in dt_final.items()}

    def in_window(dts, vf, vt, window):
        for d in window:
            if (vf and d < vf) or (vt and d > vt):
                continue
            for x in dts:
                if x in dt_final:
                    if d in dt_final[x]:
                        return True
                else:
                    dows = daytype_dows.get(x)
                    if dows is None or d.weekday() in dows:
                        return True
        return False

    def expired(dts, vf, vt):
        if vt and vt < today:
            return True
        if dts and all(x in dt_max for x in dts):
            mx = [dt_max[x] for x in dts if dt_max[x]]
            return not mx or max(mx) < today
        return False

    def key_of(oid):
        return aliases.get(oid, oid)

    per = {}

    def slot(oid):
        k = key_of(oid or "SENZA_OPERATORE")
        if k not in per:
            per[k] = {"name": ops.get(oid) or auths.get(oid) or oid or "SENZA_OPERATORE", "ids": set(), "lines": set(),
                      "line_names": set(), "modes": collections.Counter(), "service_journeys": 0,
                      "sj_active_now": 0, "sj_active_horizon": 0, "sj_expired": 0}
        per[k]["ids"].add(oid or "SENZA_OPERATORE")
        return per[k]

    for lid, l in lines.items():
        p = slot(l["op"])
        p["lines"].add(lid)
        p["line_names"].add(l["name"])
        p["modes"][l["mode"] or "?"] += 1
    unattributed = 0
    for opr, lr, sjp, dts, vf, vt in sjs:
        line = lr if lr in lines else None
        if line is None and sjp:
            r = sjp_line.get(sjp)
            line = r if r in lines else route_line.get(r)
        if line is None:
            unattributed += 1
        p = slot(opr or (lines.get(line) or {}).get("op"))
        p["service_journeys"] += 1
        if in_window(dts, vf, vt, w_now):
            p["sj_active_now"] += 1
        if in_window(dts, vf, vt, w_hor):
            p["sj_active_horizon"] += 1
        if expired(dts, vf, vt):
            p["sj_expired"] += 1
    for oid, nm in ops.items():  # operatori dichiarati senza linee (es. sharing)
        slot(oid)
    operators = {}
    for k, p in sorted(per.items(), key=lambda kv: kv[1]["name"].lower()):
        operators[k] = {"name": p["name"], "ids": sorted(p["ids"]), "lines": len(p["lines"]),
                        "service_journeys": p["service_journeys"], "sj_active_now": p["sj_active_now"],
                        "sj_active_horizon": p["sj_active_horizon"], "sj_expired": p["sj_expired"],
                        "modes": dict(p["modes"]), "line_names": sorted(p["line_names"])}
    tot = {"Line": len(lines), "StopPlace": stopplaces, "ServiceJourney": len(sjs),
           "sj_active_now": sum(o["sj_active_now"] for o in operators.values()),
           "sj_active_horizon": sum(o["sj_active_horizon"] for o in operators.values()),
           "sj_expired": sum(o["sj_expired"] for o in operators.values()),
           "DayType": len(daytype_dows), "unattributed_sj": unattributed}
    tot["sj_expired_pct"] = round(tot["sj_expired"] / tot["ServiceJourney"] * 100, 1) if tot["ServiceJourney"] else 0.0
    return {"file": zip_path.replace("\\", "/").rsplit("/", 1)[-1], "seconds": round(time.time() - t0),
            "ref_date": str(today),
            "windows": {"now": [str(min(w_now)), str(max(w_now))], "horizon": [str(min(w_hor)), str(max(w_hor))]},
            "totals": tot, "operators": operators}


def compact(stats):
    """Versione senza elenco linee, adatta a state.json."""
    out = {k: v for k, v in stats.items() if k != "operators"}
    out["operators"] = {k: {kk: vv for kk, vv in o.items() if kk != "line_names"} for k, o in stats["operators"].items()}
    return out


def compare(prev, cur, sj_drop_warn_pct=40, metric="sj_active_horizon"):
    """Confronta due analisi (precedente, corrente) dello stesso livello. Avvisi:
    - operatore sparito: aveva linee e ora non ne ha (o non c'e' piu')
    - calo corse: `metric` (default corse attive a 2-3 settimane) in calo >= sj_drop_warn_pct.
    Se l'analisi precedente non ha la metrica (formato vecchio) usa service_journeys."""
    warnings, rows = [], []
    p_ops, c_ops = (prev or {}).get("operators", {}), (cur or {}).get("operators", {})
    for k in sorted(set(p_ops) | set(c_ops), key=lambda x: ((c_ops.get(x) or p_ops.get(x))["name"]).lower()):
        p, c = p_ops.get(k), c_ops.get(k)
        pn, cn = (p or {}).get("name"), (c or {}).get("name")
        name = f"{cn} (ex {pn})" if pn and cn and pn != cn else (cn or pn or k)
        m = metric if (p or {}).get(metric) is not None or not p else "service_journeys"
        lp, lc = (p or {}).get("lines", 0), (c or {}).get("lines", 0)
        vp, vc = (p or {}).get(m, 0) or 0, (c or {}).get(m, 0) or 0
        change = round((vc - vp) / vp * 100, 1) if vp else None
        if p and lp > 0 and (not c or lc == 0):
            stato = "SPARITO"
            warnings.append(f"operatore sparito: {name} ({lp} linee, {vp:,} corse attive nello snapshot precedente)")
        elif not p and c:
            stato = "NUOVO"
        elif lp == 0 and lc == 0:
            stato = "senza linee"
        elif change is not None and change <= -sj_drop_warn_pct:
            stato = "CALO CORSE"
            warnings.append(f"operatore {name}: corse attive {vp:,} -> {vc:,} ({change:+.1f}%), linee {lp} -> {lc}")
        else:
            stato = "ok"
        rows.append({"operatore": name, "key": k, "lines_prev": lp, "lines_cur": lc, "sj_prev": vp, "sj_cur": vc,
                     "sj_change_pct": change, "metric": m, "stato": stato,
                     "expired_cur": (c or {}).get("sj_expired"), "sj_total_cur": (c or {}).get("service_journeys")})
    return warnings, rows


def level_consistency(l1, l2, tol_pct=10, metric="sj_active_horizon"):
    """Verifica che il servizio del livello 1 sia contenuto nel livello 2 (stessa data di riferimento).
    Avviso per ogni operatore con corse attive in L1 assente o con meno del (100 - tol_pct)% in L2."""
    warnings = []
    o1, o2 = (l1 or {}).get("operators", {}), (l2 or {}).get("operators", {})
    for k, a in o1.items():
        v1 = a.get(metric) or 0
        if v1 <= 0:
            continue
        v2 = (o2.get(k) or {}).get(metric) or 0
        if v2 < v1 * (1 - tol_pct / 100):
            if v2 == 0:
                warnings.append(f"incoerenza livelli: {a['name']} ha {v1:,} corse attive in L1 e nessuna in L2")
            else:
                warnings.append(f"incoerenza livelli: {a['name']} ha {v1:,} corse attive in L1 e solo {v2:,} in L2 ({(v2 - v1) / v1 * 100:+.0f}%)")
    return warnings


def format_table(rows, prev_label="prec.", cur_label="corr."):
    out = [f"{'operatore':<34}{'linee ' + prev_label:>13}{'linee ' + cur_label:>13}{'attive ' + prev_label:>15}{'attive ' + cur_label:>15}{'var.':>9}{'scadute':>9}  stato"]
    for r in rows:
        ch = f"{r['sj_change_pct']:+.1f}%" if r["sj_change_pct"] is not None else "-"
        sc = f"{r['expired_cur']:,}" if r.get("expired_cur") is not None else "-"
        out.append(f"{r['operatore'][:33]:<34}{r['lines_prev']:>13}{r['lines_cur']:>13}{r['sj_prev']:>15,}{r['sj_cur']:>15,}{ch:>9}{sc:>9}  {r['stato']}")
    return "\n".join(out)

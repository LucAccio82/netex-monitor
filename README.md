# netex-monitor

Monitoraggio quotidiano degli snapshot **NeTEx** pubblicati dal NAP CCISS per la sperimentazione **MaaS4Italy**
(`https://www.cciss.it/nap/public/api/v1/netex/db/snapshot`), livelli 1 e 2, per le regioni in `config.json`.

Ogni giorno un workflow GitHub Actions controlla se ci sono nuove versioni; per ognuna scarica lo zip, ne analizza
il contenuto e salva qui i risultati. Il build in OpenTripPlanner (che richiede ~20 GB di RAM) per ora resta sul PC locale.

## Cosa viene controllato

Per ogni nuova versione di ciascun livello:

| Misura | Significato |
|---|---|
| operatori, linee, corse | conteggi del feed, per operatore |
| corse attive 7 gg | corse con almeno un giorno di servizio nei prossimi 7 giorni |
| **corse attive a 2-3 settimane** | corse con almeno un giorno di servizio tra il 14° e il 20° giorno: il feed copre il servizio fino al prossimo aggiornamento? |
| corse scadute | corse senza piu' giorni di servizio (validita' o calendario terminati) |

Il calendario e' ricostruito da DayType → DayTypeAssignment → UicOperatingPeriod / OperatingPeriod / Date,
intersecato con il `ValidBetween` della corsa.

**Avvisi** (stato `feed_warning`):
- *contenuto*: corse attive a 2-3 settimane almeno 15% sotto la **baseline** della regione (il feed piu' completo
  conosciuto: si imposta da sola, sale da sola se il feed cresce oltre il 15%, non scende mai);
- *operatori*: operatore sparito o con corse attive in calo del 40% rispetto alla versione precedente dello stesso livello;
- *scadenze*: oltre il 20% di corse scadute, operatori con orari in scadenza prima della finestra a 2-3 settimane,
  operatori con linee ma nessuna corsa attiva;
- *coerenza L1 ⊆ L2*: servizio attivo presente nel livello 1 e assente o ridotto (oltre il 10%) nel livello 2.

## Dove guardare

- `risultati/last_run.json` – esito dell'ultimo controllo
- `risultati/history.csv` – storico (separatore `;`, apribile in Excel)
- `risultati/netex_monitor.log` – log (ultime 3000 righe)
- scheda **Actions** – esecuzioni del workflow; un'esecuzione fallita viene notificata via email da GitHub

## Struttura

```
config.json            regioni, livelli, soglie, alias degli operatori
baseline.json          baseline per regione (modificabile a mano)
script/check_netex.py      controllo giornaliero
script/netex_operators.py  analisi del feed per operatore
script/confronta_netex.py  confronto manuale tra due snapshot
interno/state.json     ultima versione vista per ogni livello (non modificare)
risultati/             esiti
.github/workflows/netex-monitor.yml   workflow pianificato
```

## Esecuzione manuale

Da GitHub: scheda **Actions** → *Monitoraggio NeTEx* → **Run workflow** (opzioni: `--dry-run` per il solo controllo
delle novita', `--force` per rianalizzare tutto).

In locale (Python 3.10+, nessuna dipendenza esterna):

```
python script/check_netex.py --dry-run
python script/confronta_netex.py <snapshot_A.zip> <snapshot_B.zip> --linee [--livelli]
```

Sul PC lo stesso codice gira con un `config.json` diverso (`build_enabled` assente/true, cartelle OTP delle regioni)
ed esegue anche il build OTP di ogni nuova versione.

## Aggiungere una regione

Aggiungere a `regions` in `config.json` un oggetto con `name`, `snapshot_base` (es. `NUT-ITI4` per il Lazio),
`levels` (`[1, 2]`) e `folder`. Al primo giro vengono scaricati e analizzati entrambi i livelli e la baseline si imposta da sola.

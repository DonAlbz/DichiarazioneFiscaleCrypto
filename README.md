# DichiarazioneFiscaleCrypto

Calcolo automatico del prezzo medio di carico e delle plusvalenza fiscalmente rilevanti, direttamente dai report binance.

I file devono essere posizionati in questo modo:

```plaintext
main.py
binance/
├── api/
│   └── api.csv
├── asset/
│   ├── 1-1-2017--31-12-2025.csv
│   └── BINANCE storico transazioni DAL 2017 AL 2026.zip
├── convert/
│   └── Esporta lo storico degli ordini-2026-02-07 13_50_04.xlsx
├── depositi/
│   └── 1-1-2021--31-12-2025.xlsx
├── earn/
│   ├── eth_stacking/
│   ├── flexible/
│   └── locked/
├── prelievi/
│   └── 1-1-2021--31-12-2025.xlsx
├── quotazioni/
│   ├── BTC-USD_2021.csv
│   ├── BTC-USD_2022.csv
│   ├── ETH-USD_2021.csv
│   └── EUR-USD_2021.csv
└── scambi/
    ├── 1-1-2021--31-12-2025.csv
    └── 1-1-2021--31-12-2025.zip
from dis import code_info
from logging import raiseExceptions

# Calcolo del prezzo medio di carico delle crypto detenute sulla base del:
# - prezzo di acquisto tramite FIAT
# - proventi da detenzione (stacking e earn)
# - vendita/acquisto tramite crypto

# Il prezzo medio di carico viene calcolato in base ai seguenti criteri:
# - nel caso di acquisto per FIAT o  e-money token, il prezzo di carico è definito dal controvalore in euro della coin
#   ed è calcolato come PMC=Somma(quantità-i * controvalore-i)/somma(quantità-i)
# - nel caso di acquisto/vendita crypto-crypto, la coin acquistata riceve il prezzo medio di carico della coin venduta,
#   solo per le quantità acquistate secondo la formula riportata al punto precedente
# - nel caso di vendita, il prezzo medio di carico rimane invariato

# librerie utilizzate:
# aiohappyeyeballs   2.6.1
# aiohttp            3.13.3
# aiosignal          1.4.0
# attrs              25.4.0
# binance            0.3.106
# certifi            2026.2.25
# charset-normalizer 3.4.5
# dateparser         1.3.0
# frozenlist         1.8.0
# idna               3.11
# multidict          6.7.1
# numpy              2.4.2
# pandas             3.0.1
# pip                25.1.1
# propcache          0.4.1
# pycryptodome       3.23.0
# python_binance     1.0.35
# python-dateutil    2.9.0.post0
# pytz               2026.1.post1
# regex              2026.2.28
# requests           2.32.5
# six                1.17.0
# typing_extensions  4.15.0
# tzdata             2025.3
# tzlocal            5.3.1
# urllib3            2.6.3
# websockets         16.0
# yarl               1.23.0




from binance.client import Client
from datetime import datetime
from collections import defaultdict, Counter
import pandas as pd
import os
import glob
import re
from datetime import timedelta

from scipy.stats import false_discovery_control

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
COINBASE_INITIAL_FILE = 'D:/730/2026/coinbase_initial_simple.csv'
BINANCE_BASE_DIR = 'D:/730/2026/binance'
BINANCE_ASSET_MASTER = 'D:/730/2026/binance/asset/1-1-2017--31-12-2025.csv'

START_DATE = "2021-01-01"
END_DATE = "2025-12-31 23:59:59"
FISCAL_YEAR_START = "2025-01-01"
FISCAL_YEAR_END = "2025-12-31 23:59:59"
# Stablecoin USD da valorizzare con EUR/USD
USD_STABLECOINS = ['USDC', 'USDT', 'BUSD', 'FDUSD']
DEBUG_COINS = {}
quotazioni = None #{[]}
pd.set_option('display.float_format', lambda x: f'{x:.8f}')
quadro_RT = []


def log_movimento(coin, coin_data, operazione, timestamp):
    if coin in DEBUG_COINS:
        print(f"  [DEBUG {coin}] {timestamp} | {operazione:<40} | "
              f"qty={coin_data[coin]['quantity']:.8f} | "
              f"cost={coin_data[coin]['total_cost']:.8f} | "
              f"PMC={coin_data[coin]['Prezzo_Medio_Di_Carico']:.8f}")

def load_asset(start_ts, end_ts, asset_dir = BINANCE_BASE_DIR + "/asset/"):
    """
       Carica asset Binance file CSV (eventualmente) multipli
       File formato: 1-1-2017--31-12-2025.csv, etc.
       Colonne: "User_ID","UTC_Time","Account","Operation","Coin","Change","Remark"
       Ritorna pandas Series con index datetime per ricerca veloce O(log n)
       """
    print("=" * 80)
    print("CARICAMENTO asset")
    print("=" * 80)

    if not os.path.exists(asset_dir):
        print(f"ERRORE: Directory asset non trovata: {asset_dir}")
        return None
    try:
        # Trova tutti i file *.csv
        files = sorted(glob.glob(os.path.join(asset_dir, '*.csv')))
        if len(files) != 1:
            print(f"ERRORE: trovati {len(files)} file .csv nella cartella {asset_dir}")
            print("Inserire solo un file .csv e ripetere l'operazione")
            return []
        file = files[0]
        print(f"Procedo al caricamento di {file}")


        if not file:
            print(f"ERRORE: Nessun file *.csv trovato in {asset_dir}")
            return None

        all_assets = {}


        df = pd.read_csv(file, float_precision='round_trip')

        # Skippa la seconda riga se contiene i ticker ripetuti
        # Identifica se la prima riga di dati contiene il ticker
        # non dovrebbe servire per gli asset, ma meglio controllare
        # if len(df) > 0 and isinstance(df.iloc[0]['UTC_Time'], str) and not df.iloc[0]['UTC_Time'].replace('-',
        #                                                                                           '').isdigit():
        #     df = df.iloc[1:].reset_index(drop=True)  # droppo la seconda riga se ticker

        print(f"File: {os.path.basename(file)}")
        print(f"Righe totali: {len(df)}")

        # Converto timestamp
        df['UTC_Time'] = pd.to_datetime(df['UTC_Time'])

        # Filtro per periodo
        df = df[(df['UTC_Time'] >= start_ts) & (df['UTC_Time'] <= end_ts)]
        print(f"   Righe nel periodo {START_DATE} - {END_DATE}: {len(df)}")

        #

        # IMPORTANTE: Rimuove dal dataframe operazioni non necessarie

        skip_operations = [
            # 'Buy',
            # 'Sell',
            # 'Fee',
            # 'Transaction Buy',
            # 'Transaction Sold',
            # 'Transaction Spend',
            # 'Transaction Revenue',
            # 'Deposit',
            # 'Withdraw',
            # 'Commission Fee Shared With You',
            'Simple Earn Flexible Subscription',
            'Simple Earn Flexible Redemption',
            'Simple Earn Locked Subscription',
            'Simple Earn Locked Redemption',
            # 'Simple Earn Flexible Interest',
            # 'Simple Earn Flexible Airdrop',
            # 'Simple Earn Locked Rewards',
            # 'Staking Rewards',
            # 'ETH 2.0 Staking Rewards',
            # 'Swap Farming Rewards'
            'Liquid Swap Add',
            'Liquidity Farming Remove'
        ]

        df_before_skip = len(df)
        df = df[~df['Operation'].isin(skip_operations)]
        print(f"Dal totale delle operazioni sono state filtrate {df_before_skip - len(df)} operazioni")
        print(f"Righe rimanenti dal master: {len(df)}")

        # elimino righe vuote
        #df = df.dropna()

        # Lista di tutte le operazioni
        operations = []
        # Contatore per ogni tipo di operazioni
        new_operations = defaultdict(int)

        for _, row in df.iterrows():
            timestamp = row['UTC_Time']
            coin = row['Coin']

            # Skippa se coin è None/NaN
            if not coin or pd.isna(coin):
                continue

            change = row['Change']
            operation = row['Operation']

           # Aggiungo l'operazione
            operations.append({
                'timestamp': timestamp,
                'operation': operation,
                'coin': coin,
                'change': change,
                'remark': row.get('Remark', ''),
                'source': file,
                'gia_elaborata': False
            })

            new_operations[operation] += 1


        # print(f"   Operazioni uniche da aggiungere: {len(operations)}")

        if len(operations) > 0:
            print(f"\n      Nuove operazioni per tipo (top 15):")
            sorted_ops = sorted(new_operations.items(), key=lambda x: -x[1])
            for op, count in sorted_ops[:15]:
                print(f"      {op:<50} {count:>5}x")
        print()
        return pd.DataFrame(operations).set_index('timestamp').sort_index()

    except Exception as e:
        print("!"*80)
        print(f"Errore caricamento CSV master: {e}\n")
        print("!" * 80)
        import traceback
        traceback.print_exc()
        return []


def get_price_at_timestamp(series, ts):
    idx = series.index.searchsorted(ts)

    if idx == 0:
        return series.iloc[0]
    elif idx >= len(series):
        return series.iloc[-1]
    else:
        before = series.index[idx - 1]
        after = series.index[idx]

        if (ts - before) <= (after - ts):
            return series.iloc[idx - 1]
        else:
            return series.iloc[idx]

def load_quotes(quotes_dir=BINANCE_BASE_DIR + "/quotazioni/"):
    """
    Carica quotazioni storiche EUR/USD da file CSV multipli (uno per anno)
    File formato: EURUSD=X_2021.csv, EURUSD=X_2022.csv, etc.
    Colonne: Date,Open,High,Low,Close,Volume
    Ritorna pandas Series con index datetime per ricerca veloce O(log n)
    """
    print("\nCaricamento quotazioni...")

    if not os.path.exists(quotes_dir):
        print(f"ERRORE: Directory quotazioni non trovata: {quotes_dir}")
        print("Userò tasso fisso 1.0 (1 USD = 1 EUR)")
        return None

    try:
        # Trova tutti i file *.csv
        files = sorted(glob.glob(os.path.join(quotes_dir, '*.csv')))

        if not files:
            print(f"ERRORE: Nessun file *.csv trovato in {quotes_dir}")
            print("Userò tasso fisso 1.0 (1 USD = 1 EUR)")
            return None

        all_quotes = {}

        for file in files:
            quote = os.path.basename(file).split('_')[0]
            year = os.path.basename(file).split('_')[1].replace('.csv', '')

           # Gestisco l'aggiunta al dizionario
            if quote not in all_quotes:
                all_quotes[quote] = []  # Crea la lista se è la prima volta che vedi questo ticker
            print(f"{quote} {year}")
            # Leggi CSV
            df = pd.read_csv(file)

            # Skippa la seconda riga se contiene i ticker ripetuti
            # Identifica se la prima riga di dati contiene il ticker
            if len(df) > 0 and isinstance(df.iloc[0]['Date'], str) and not df.iloc[0]['Date'].replace('-',
                                                                                                      '').isdigit():
                df = df.iloc[1:].reset_index(drop=True) #droppo la seconda riga se ticker

            # Converti Date in datetime
            df['Date'] = pd.to_datetime(df['Date'])

            # Converti Close in float
            df['Close'] = pd.to_numeric(df['Close'], errors='coerce')

            # Usa la colonna Close come tasso di cambio
            df = df[['Date', 'Close']].copy()
            df = df.dropna() #elimin righe vuote

            all_quotes[quote].append(df)

        # Dizionario per contenere le Series finali (es. quotes_series["USDC-EUR"])
        quotes_series = {}

        # Combina tutti gli anni per ogni ticker
        for quote in all_quotes:
            # Unisce i vari DataFrame annuali in uno solo
            combined = pd.concat(all_quotes[quote], ignore_index=True)

            # Rimuove eventuali duplicati (se gli anni si sovrappongono) e ordina per data
            combined = combined.drop_duplicates('Date').sort_values('Date')

            # Crea la Series: index=Date, value=Close
            # La salviamo nel dizionario usando il nome del ticker come chiave
            s = combined.set_index('Date')['Close']

            # Assicurati che l'indice sia ordinato per la ricerca veloce
            quotes_series[quote] = s.sort_index()

            print(f"Caricate {len(quotes_series[quote]):,} quotazioni {quote}")
            print(
                f"Periodo: {quotes_series[quote].index[0].strftime('%Y-%m-%d')} → {quotes_series[quote].index[-1].strftime('%Y-%m-%d')}")

        return quotes_series

    except Exception as e:
        print(f"Errore caricamento quotazioni: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_amount_with_currency(value_str):
    """
    Estrae numero e valuta da stringhe come '965.6EUR' o '1144.62224USDC'
    Ritorna (numero_float, valuta_str)
    """
    if pd.isna(value_str):
        return 0.0, ''

    value_str = str(value_str).strip()

    # Regex: cattura numero (con decimali) seguito da lettere
    match = re.match(r'^([\d.]+)([A-Z]+)$', value_str)

    if match:
        number = float(match.group(1))
        currency = match.group(2)
        return number, currency
    else:
        # Prova a convertire direttamente a float
        try:
            return float(value_str), ''
        except:
            print(f"ERRORE: dalla transazione {value_str} non è stato possibile dedurre quantità e valuta")
            return 0.0, ''


def extract_base_quote_from_pair(pair):
    """Estrae base e quote da pair (es. BTCUSDT -> BTC, USDT)"""
    quote_assets = ['USDT', 'BUSD', 'USDC', 'EUR', 'BTC', 'BETH', 'ETH', 'BNB', 'FDUSD']

    for quote in quote_assets:
        if pair.endswith(quote):
            # prende tutto il pair, tranne gli ultimi len(quote) caratteri
            base = pair[:-len(quote)]
            return base, quote

    # Fallback
    print(f"ERRORE: dalla coppia {pair} non è stato possibile dedurre le valute")
    if len(pair) > 6:
        return pair[:-4], pair[-4:]
    else:
        return pair[:-3], pair[-3:]


def load_scambi(base_dir):
    """
    Carica scambi da CSV
    IMPORTANTE: Ogni trade genera MULTIPLE righe nel CSV master (Buy, Sell, Fee)
    Quindi per ogni trade va verificato quale coin è stata acquistata e quale venduta
    """
    operations = []
    scambi_dir = os.path.join(base_dir, 'scambi')

    if not os.path.exists(scambi_dir):
        print(f"    Directory scambi non trovata: {scambi_dir}")
        return operations

    print("Caricamento SCAMBI:")

    files = glob.glob(os.path.join(scambi_dir, '*.csv'))

    if not files:
        print(f"   Nessun file CSV trovato in {scambi_dir}")
        return operations

    total_loaded = 0
    total_errors = 0

    for file in files:
        try:
            df = pd.read_csv(file, float_precision='round_trip')
            file_ops = 0

            # DEBUG: Mostra prime righe
            # print(f"\n   DEBUG - Prime 3 righe di {os.path.basename(file)}:")
            # for _, row in df.head(3).iterrows():
            #     print(f"      {row['Date(UTC)']} | {row['Pair']} | {row['Side']}")

            # Colonne: Date(UTC), Pair, Side, Price, Executed, Amount, Fee

            for idx, row in df.iterrows():
                try:
                    timestamp = pd.to_datetime(row['Date(UTC)'])
                    pair = row['Pair']
                    side = row['Side']

                    # Parse Executed (es. '965.6EUR')
                    executed_val, executed_coin = parse_amount_with_currency(row['Executed'])

                    # Parse Amount (es. '1144.62224USDC')
                    amount_val, amount_coin = parse_amount_with_currency(row['Amount'])

                    # Parse Fee
                    fee_val, fee_coin = parse_amount_with_currency(row['Fee'])

                    # Determina base e quote dal pair
                    base, quote = extract_base_quote_from_pair(pair)

                    if side == 'BUY':
                        # BUY: COMPRI la BASE del pair PAGANDO la QUOTE
                        # Esempio: BUY POLUSDT @ 0.3998
                        #   - Executed: 375POL → AGGIUNGERE (ricevi POL)
                        #   - Amount: 149.925USDT → SOTTRARRE (paghi USDT)
                        #   - Fee: 0.375POL → SOTTRARRE (paghi fee in POL)
                        operations.append({
                            'timestamp': timestamp,
                            'operation': 'BUY',
                            'coin': executed_coin if executed_coin else base,  # BASE: da AGGIUNGERE
                            'change': executed_val,  # Positivo
                            'quote_coin': amount_coin if amount_coin else quote,  # QUOTE: da SOTTRARRE
                            'quote_amount': amount_val,
                            'fee': fee_val,
                            'fee_coin': fee_coin,
                            'source': 'scambi',
                            'gia_elaborata': False
                            # IMPORTANTE: Aggiungi fingerprint multipli per deduplicazione con master
                            # 'master_fingerprints': [
                            #     # 1. Buy coin (executed) - positivo
                            #     f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{executed_coin}|{executed_val:.8f}",
                            #     # 2. Sell quote_coin (amount) - negativo nel master
                            #     f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{amount_coin}|{-amount_val:.8f}",
                            #     # 3. Fee - negativo
                            #     f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{fee_coin}|{-fee_val:.8f}" if fee_val > 0 else None
                            # ]
                        })
                    else:  # SELL
                        # SELL: VENDI la BASE del pair RICEVENDO la QUOTE
                        # Esempio: SELL EURUSDC @ 1.1854
                        #   - Executed: 965.6EUR → SOTTRARRE (vendi EUR)
                        #   - Amount: 1144.62USDC → AGGIUNGERE (ricevi USDC)
                        #   - Fee: 1.14USDC → SOTTRARRE (paghi fee in USDC)
                        operations.append({
                            'timestamp': timestamp,
                            'operation': 'SELL',
                            'coin': executed_coin if executed_coin else base,  # BASE: da SOTTRARRE
                            'change': -executed_val,  # Negativo
                            'quote_coin': amount_coin if amount_coin else quote,  # QUOTE: da AGGIUNGERE
                            'quote_amount': amount_val,
                            'fee': fee_val,
                            'fee_coin': fee_coin,
                            'source': 'scambi',
                            'gia_elaborata': False
                            # IMPORTANTE: Fingerprint multipli
                            # 'master_fingerprints': [
                            #     # 1. Sell coin (executed) - negativo
                            #     f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{executed_coin}|{-executed_val:.8f}",
                            #     # 2. Buy quote_coin (amount) - positivo nel master
                            #     f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{amount_coin}|{amount_val:.8f}",
                            #     # 3. Fee - negativo
                            #     f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{fee_coin}|{-fee_val:.8f}" if fee_val > 0 else None
                            # ]
                        })

                    # Conteggio (fuori da if/else - conta sia BUY che SELL)
                    file_ops += 1
                    total_loaded += 1

                except Exception as e:
                    total_errors += 1
                    if total_errors <= 3:  # Mostra solo i primi 3 errori
                        print(f"    ️  Errore riga {idx}: {e}")

            print(f"   ✓ {os.path.basename(file)}: {file_ops} scambi caricati (su {len(df)} righe)")

        except Exception as e:
            print(f"     Errore file {os.path.basename(file)}: {e}")

    if total_errors > 3:
        print(f"   ️  ...e altri {total_errors - 3} errori")

    print(f"   TOTALE: {total_loaded} scambi caricati da {len(files)} file\n")

    return operations

def deposita_coin(c, coin_data, qty, timestamp, coin_a_pmc_zero):
    # Altri depositi: costo = 0 per crypto (acquisite gratuitamente o da fonti esterne)
    # Eccezioni: EUR = 1, USD stablecoin = tasso storico
    coin_data[c]['quantity'] += qty
    if c == 'EUR':
        coin_data[c]['total_cost'] += qty
    elif c == "USDC" and quotazioni is not None:
        print(f"rilevato deposito USDC in data {timestamp}")
        pmc_deposito = input("Inserisci prezzo medio di carico in EUR, oppure N se non disponibile: ")

        if pmc_deposito.upper() == 'N':
            pmc_deposito = 0
        else:
            try:
                pmc_deposito = float(pmc_deposito)
            except ValueError:
                raise Exception("Valore inserito non valido: inserire un numero o 'N'")

        # caso in cui ho ricevuto un deposito del quale conosco il prezzo medio di carico
        # rate = get_price_at_timestamp(quotazioni['USDC-EUR'],pd.to_datetime(timestamp).normalize())
        coin_data[c]['total_cost'] += qty * pmc_deposito# if pmc_deposito > 0 else raise Exception(f"Non trovato prezzo medio di carico di {c} al tempo {timestamp}")
        # print(f"Aggiunto operazione {op_type} per la coint {coin}")
        # print(f"Trovata quotazione USDC-EUR pari a {rate}")
        # print(f"Il costo totale coin passa a {coin_data[c]['total_cost']}")
        # print(f"Il prezzo medio di carico è: {coin_data[c]['total_cost'] / coin_data[c]['quantity']}")

    elif c in USD_STABLECOINS and quotazioni is not None:
        print(f"rilevato deposito nella stablecoin {c} in data {timestamp}")
        pmc_deposito = input("Inserisci prezzo medio di carico, oppure N se non disponibile: ")

        if pmc_deposito.upper() == 'N':
            pmc_deposito = 0
        else:
            try:
                pmc_deposito = float(pmc_deposito)
            except ValueError:
                raise Exception("Valore inserito non valido: inserire un numero o 'N'")

        #rate = get_price_at_timestamp(quotazioni['USDC-EUR'], pd.to_datetime(timestamp).normalize())
        coin_data[c]['total_cost'] += qty * pmc_deposito #if rate > 0 else qty
    else:
        if not coin_a_pmc_zero:
            print(f"rilevato deposito nella token coin {c} in data {timestamp}")
            pmc_deposito = input("Inserisci prezzo medio di carico in EUR, oppure N se non disponibile: ")

            if pmc_deposito.upper() == 'N':
                pmc_deposito = 0
            else:
                try:
                    pmc_deposito = float(pmc_deposito)
                except ValueError:
                    raise Exception("Valore inserito non valido: inserire un numero o 'N'")
        else:
            pmc_deposito = 0
        # rate = quotazioni['EUR-USD'], pd.to_datetime(timestamp).normalize() #caso valore in USD
        coin_data[c]['total_cost'] += qty * pmc_deposito if pmc_deposito > 0 else raiseExceptions
    coin_data[c]['Prezzo_Medio_Di_Carico'] = coin_data[c]['total_cost'] / coin_data[c]['quantity']
    # coin_data[c]['gia_elaborata'] = True
    log_movimento(c, coin_data, f"Deposit", timestamp)


def preleva_coin(c, coin_data,qty, timestamp):
    #nell'asset il prelievo ha già segno negativo
    coin_data[c]['quantity'] += qty
    log_movimento(c, coin_data, f"Withraw", timestamp)

def elabora_binance_convert(coin, change, timestamp, assets, coin_data, quadro_RT, is_fiscal):
    col_idx = assets.columns.get_loc('gia_elaborata')

    # cerca la riga controparte nello stesso secondo
    i_start, i_end = assets.index.slice_locs(timestamp, timestamp)
    controparte = None
    controparte_pos = None

    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] == 'Binance Convert' and
                row['coin'] != coin and
                not row['gia_elaborata']):
            controparte = row
            controparte_pos = pos
            break

    # se non trovata, cerca ±1 secondo
    if controparte is None:
        for delta in [-1, 1]:
            ts_cerca = timestamp + timedelta(seconds=delta)
            i_start, i_end = assets.index.slice_locs(ts_cerca, ts_cerca)
            for pos in range(i_start, i_end):
                row = assets.iloc[pos]
                if (row['operation'] == 'Binance Convert' and
                        row['coin'] != coin and
                        not row['gia_elaborata']):
                    controparte = row
                    controparte_pos = pos
                    break
            if controparte is not None:
                break

    if controparte is None:
        raise Exception(f"Binance Convert: controparte non trovata per {coin} {change} al {timestamp}")

    # determina quale è la coin venduta e quale quella ricevuta
    if change < 0:
        # questa riga è la coin venduta
        c_venduta = coin
        qty_venduta = abs(change)
        coin_ricevuta = controparte['coin']
        qty_ricevuta = abs(controparte['change'])
    else:
        # questa riga è la coin ricevuta, la controparte è quella venduta
        c_venduta = controparte['coin']
        qty_venduta = abs(controparte['change'])
        coin_ricevuta = coin
        qty_ricevuta = abs(change)

    # calcolo fiscale: la coin ricevuta eredita il costo della coin venduta
    pmc_venduta = coin_data[c_venduta]['Prezzo_Medio_Di_Carico']
    costo_venduta = qty_venduta * pmc_venduta

    # aggiorno coin venduta
    coin_data[c_venduta]['quantity'] -= qty_venduta
    coin_data[c_venduta]['total_cost'] -= costo_venduta
    # PMC coin venduta rimane invariato per proprietà matematica

    # aggiorno coin ricevuta
    coin_data[coin_ricevuta]['quantity'] += qty_ricevuta
    coin_data[coin_ricevuta]['total_cost'] += costo_venduta

    if coin_ricevuta == 'EUR':
        coin_data[coin_ricevuta]['Prezzo_Medio_Di_Carico'] = 1
    elif coin_data[coin_ricevuta]['quantity'] > 0:
        coin_data[coin_ricevuta]['Prezzo_Medio_Di_Carico'] = (
                coin_data[coin_ricevuta]['total_cost'] / coin_data[coin_ricevuta]['quantity']
        )

    log_movimento(c_venduta, coin_data, f"Binance Convert vende {c_venduta}", timestamp)
    log_movimento(coin_ricevuta, coin_data, f"Binance Convert riceve {coin_ricevuta}", timestamp)

    # setto entrambe le righe come già elaborate
    # riga corrente
    i_start, i_end = assets.index.slice_locs(timestamp, timestamp)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] == 'Binance Convert' and
                row['coin'] == coin and
                row['change'] == change and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break

    # riga controparte
    assets.iloc[controparte_pos, col_idx] = True
    # compilazione quadro_RT
    if is_fiscal:
        if coin_ricevuta == 'EUR':
            corrispettivo_eur = qty_ricevuta
        elif coin_ricevuta == 'USDC' and quotazioni is not None:
            rate = get_price_at_timestamp(quotazioni['USDC-EUR'], pd.to_datetime(timestamp).normalize())
            corrispettivo_eur = qty_ricevuta * rate
        elif c_venduta == 'EUR':
            corrispettivo_eur = qty_venduta
        else:
            corrispettivo_eur = None

        costo_fiscale = qty_venduta * pmc_venduta
        plusvalenza = (corrispettivo_eur - costo_fiscale) if corrispettivo_eur is not None else None

        quadro_RT.append({
            'data': timestamp,
            'operazione': 'Binance Convert',
            'coin_ceduta': c_venduta,
            'qty_ceduta': qty_venduta,
            'pmc_coin_ceduta': pmc_venduta,
            'costo_fiscale': costo_fiscale,
            'coin_ricevuta': coin_ricevuta,
            'qty_ricevuta': qty_ricevuta,
            'corrispettivo_eur': corrispettivo_eur,
            'plusvalenza': plusvalenza
        })


def elabora_airdrop(coin, change, timestamp, coin_data, assets, op_type, quadro_RT, is_fiscal):
    # aumenta la quantità a costo zero — il PMC si abbassa automaticamente
    coin_data[coin]['quantity'] += change

    # ricalcola PMC (si abbassa perché aggiungiamo quantità a costo zero)
    if coin_data[coin]['quantity'] > 0:
        coin_data[coin]['Prezzo_Medio_Di_Carico'] = (
                coin_data[coin]['total_cost'] / coin_data[coin]['quantity']
        )

    log_movimento(coin, coin_data, f"{op_type}", timestamp)

    # setto la riga come già elaborata
    col_idx = assets.columns.get_loc('gia_elaborata')
    i_start, i_end = assets.index.slice_locs(timestamp, timestamp)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] == op_type and
                row['coin'] == coin and
                row['change'] == change and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break

# le reward le considero come airdrop
def elabora_reward(coin, change, timestamp, coin_data, assets, op_type, quadro_RT, is_fiscal):
    elabora_airdrop(coin, change, timestamp, coin_data, assets, op_type, quadro_RT, is_fiscal)

def elabora_buy(scambio, coin_data, timestamp, assets, i, quadro_RT, is_fiscal):
    # # discriminare il funzionamento in base al tipo di scambio
    start = timestamp - timedelta(minutes=10)
    end = timestamp + timedelta(minutes=10)
    c = scambio["coin"]
    qty = scambio["change"]
    #
    # # cerca in scambi lo scambio BUY corrispondente
    # risultati = scambi[
    #     (scambi['operation'] == ['BUY']) &
    #     (scambi['coin'] == c) &
    #     (scambi['timestamp'].between(start, end)) &
    #     (scambi['change'] == qty) &
    #     (scambi['gia_elaborata'] == False)
    # ]
    # if len(risultati) > 1:
    #     print(f"Trovate più scambi BUY nel periodo {timestamp}, con la coin {c} di importo {qty}:")
    #     print(risultati.to_string())
    #     raise Exception("Mi fermo perché non so quale scambio elaborare tra i tanti trovati")
    # if len(risultati) == 0:
    #     print(f"Nessuno scambio BUY trovato nel periodo {timestamp}, con la coin {c} di importo {qty}:")
    #     raise Exception("Mi fermo perché non ho trovato uno scambio al quale associare l'operazione")


    # seleziono la coin venduta
    coin_venduta = scambio['quote_coin']

    # calcolo la quantità di coin venduta da sottrarre
    qty_coin_venduta = scambio['quote_amount']

    # decremento quantità coin venduta
    coin_data[coin_venduta]['quantity'] -= qty_coin_venduta

    # if (coin_venduta == "ETH" or c == "BETH"):
    #     print(f"{timestamp} tolti {qty_coin_venduta} ETH. ETH tot: {coin_data[coin_venduta]['quantity']}")

    # recupero il PMC della coin venduta
    pmc_coin_venduta = coin_data[coin_venduta]['Prezzo_Medio_Di_Carico']

    # calcolo il costo della coin venduta per la quantità venduta
    costo_coin_venduta = qty_coin_venduta * pmc_coin_venduta

    # verifico coin fee
    fee_coin = scambio['fee_coin']

    # salvo quantità fee
    qty_fee = scambio['fee']


    # Se la fee è nella stessa coin della coin acquistata:
    if fee_coin == c:

        # incremento quantità coin acquistata
        coin_data[c]['quantity'] += qty - qty_fee
        # if (c =="ETH" or c == "BETH"):
        #     print(f"{timestamp} aggiunti {qty - qty_fee} ETH. ETH tot: {coin_data[c]['quantity']}")


    else:
        #Se la fee è in una coin diversa dalla coin acquistata:

        # incremento quantità coin acquistata
        coin_data[c]['quantity'] += qty
        # if (c =="ETH" or c == "BETH"):
        #     print(f"{timestamp} aggiunti {qty} ETH. ETH tot: {coin_data[c]['quantity']}")

        #calcolo valore fiscale della coin venduta
        valore_fiscale_fee = qty_fee * coin_data[fee_coin]['Prezzo_Medio_Di_Carico']

        # calcolo il costo della coin venduta aggiungendo il valore fiscale della fee
        costo_coin_venduta += valore_fiscale_fee

        # modifico quantità coin fee
        coin_data[fee_coin]['quantity'] -= qty_fee

    # aggiorno il costo totale della coin venduta (il suo PMC rimane uguale per proprietà matematica)
    coin_data[coin_venduta]['total_cost'] -= costo_coin_venduta

    # assegno il nuovo costo totale alla coin acquistata
    coin_data[c]['total_cost'] += costo_coin_venduta

    # calcolo il nuovo PMC della coin acquistata
    #print(f'Sto calcolando il PMC di {c} acquistata in data {timestamp}')
    if c != "EUR":
        coin_data[c]['Prezzo_Medio_Di_Carico'] = coin_data[c]['total_cost'] / coin_data[c]['quantity']
    else:
        coin_data[c]['Prezzo_Medio_Di_Carico'] = 1

    # setto lo scambio come già elaborato
    scambi.loc[scambio.name, 'gia_elaborata'] = True

    # individuo la colonna del campo gia_elaborata (mi serve per settare gia elaborata la fee e la coin venduta)
    col_idx = assets.columns.get_loc('gia_elaborata')

    # --- cerco e setto la coin fee come già elaborata ---
    #print(f"Sto cercando {fee_coin} a un importo di {-qty_fee}")
    i_start, i_end = assets.index.slice_locs(start, end)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] in ['Fee', 'Transaction Fee'] and
                row['coin'] == fee_coin and
                row['change'] == (-1 * qty_fee) and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break

    # --- setto la coin ACQUISTATA (BUY) come già elaborata ---
    i_start, i_end = assets.index.slice_locs(timestamp, timestamp)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] in ['Buy', 'Transaction Buy', 'Transaction Revenue'] and
                row['coin'] == c and
                row['change'] == qty and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break

    # --- cerco e setto la coin VENDUTA (SELL) come già elaborata ---
    #print(f"Sto cercando {coin_venduta} a un importo di {-qty_coin_venduta}")
    i_start, i_end = assets.index.slice_locs(start, end)
    trovata = False
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] in ['Sell', 'Transaction Spend', 'Transaction Sold'] and
                row['coin'] == coin_venduta and
                row['change'] == (-1 * qty_coin_venduta) and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            trovata = True
            break

    if not trovata:
        # DEBUG: stampa i valori esatti per capire il mismatch
        i_start, i_end = assets.index.slice_locs(start, end)
        print(f"Cerco: coin={coin_venduta}, change={repr(-1 * qty_coin_venduta)}")
        print(f"Righe nel range temporale:")
        for pos in range(i_start, i_end):
            row = assets.iloc[pos]
            if row['coin'] == coin_venduta:
                print(f"  operation={row['operation']}, change={repr(row['change'])}, "
                      f"uguale={row['change'] == (-1 * qty_coin_venduta)}, "
                      f"differenza={abs(row['change'] - (-1 * qty_coin_venduta))}")
        raise Exception(...)
    # if not trovata:
    #     print(f"Nessuno scambio SELL trovato nel periodo {timestamp}, con la coin {coin_venduta} di importo {qty_coin_venduta}:")
    #     raise Exception("Mi fermo perché non ho trovato uno scambio al quale associare l'operazione")
    log_movimento(c, coin_data, f"BUY compra {c}", timestamp)
    log_movimento(coin_venduta, coin_data, f"BUY vende {coin_venduta}", timestamp)
    log_movimento(fee_coin, coin_data, f"BUY fee {fee_coin}", timestamp)

    # salva in quadro_RT se nell'anno fiscale e coin ottenuta è EUR o USDC
    if is_fiscal:
        # in un BUY stai cedendo coin_venduta per ricevere c
        # il corrispettivo è il valore in EUR della coin ricevuta
        if c == 'EUR':
            corrispettivo_eur = qty - (qty_fee if fee_coin == c else 0)
        elif c == 'USDC' and quotazioni is not None:
            rate = get_price_at_timestamp(quotazioni['USDC-EUR'], pd.to_datetime(timestamp).normalize())
            qty_netta = qty - qty_fee if fee_coin == c else qty
            corrispettivo_eur = qty_netta * rate
        else:
            corrispettivo_eur = None

        plusvalenza = (corrispettivo_eur - costo_coin_venduta) if corrispettivo_eur is not None else None

        quadro_RT.append({
            'data': timestamp,
            'operazione': 'BUY',
            'coin_ceduta': coin_venduta,
            'qty_ceduta': qty_coin_venduta,
            'pmc_coin_ceduta': pmc_coin_venduta,
            'costo_fiscale': costo_coin_venduta,
            'coin_ricevuta': c,
            'qty_ricevuta': qty - (qty_fee if fee_coin == c else 0),
            'corrispettivo_eur': corrispettivo_eur,
            'plusvalenza': plusvalenza
        })
    return 0

# i è la posizione della coin c dentro assets
def elabora_sell(scambio, coin_data, timestamp, assets, i, quadro_RT, is_fiscal):
    start = timestamp - timedelta(minutes=10)
    end = timestamp + timedelta(minutes=10)

    c_venduta = scambio['coin']
    qty_venduta = abs(scambio['change'])
    coin_ricevuta = scambio['quote_coin']
    qty_ricevuta = scambio['quote_amount']
    fee_coin = scambio['fee_coin']
    qty_fee = scambio['fee']

    # PMC della coin venduta — rimane invariato
    pmc_coin_venduta = coin_data[c_venduta]['Prezzo_Medio_Di_Carico']
    costo_coin_venduta = qty_venduta * pmc_coin_venduta

    # decremento quantità e costo della coin venduta
    coin_data[c_venduta]['quantity'] -= qty_venduta
    coin_data[c_venduta]['total_cost'] -= costo_coin_venduta

    # gestione coin ricevuta e fee
    if fee_coin == coin_ricevuta:
        qty_netta_ricevuta = qty_ricevuta - qty_fee
        coin_data[coin_ricevuta]['quantity'] += qty_netta_ricevuta
        coin_data[coin_ricevuta]['total_cost'] += costo_coin_venduta
    else:
        coin_data[coin_ricevuta]['quantity'] += qty_ricevuta
        coin_data[coin_ricevuta]['total_cost'] += costo_coin_venduta
        valore_fiscale_fee = qty_fee * coin_data[fee_coin]['Prezzo_Medio_Di_Carico']
        coin_data[fee_coin]['quantity'] -= qty_fee
        coin_data[fee_coin]['total_cost'] -= valore_fiscale_fee

    # aggiorno PMC coin ricevuta
    if coin_ricevuta == 'EUR':
        coin_data[coin_ricevuta]['Prezzo_Medio_Di_Carico'] = 1
    elif coin_data[coin_ricevuta]['quantity'] > 0:
        coin_data[coin_ricevuta]['Prezzo_Medio_Di_Carico'] = (
            coin_data[coin_ricevuta]['total_cost'] / coin_data[coin_ricevuta]['quantity']
        )

    # print(f'Elaborata vendita {c_venduta} → {coin_ricevuta} in data {timestamp}')

    # --- setto lo SCAMBIO come già elaborato ---
    scambi.loc[scambio.name, 'gia_elaborata'] = True

    col_idx = assets.columns.get_loc('gia_elaborata')

    # --- setto la coin VENDUTA in assets come già elaborata ---
    i_start, i_end = assets.index.slice_locs(timestamp, timestamp)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] in ['Sell', 'Transaction Spend', 'Transaction Sold'] and
                row['coin'] == c_venduta and
                row['change'] == -qty_venduta and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break

    # --- setto la coin RICEVUTA in assets come già elaborata ---
    i_start, i_end = assets.index.slice_locs(start, end)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] in ['Buy', 'Transaction Buy' , 'Transaction Revenue'] and
                row['coin'] == coin_ricevuta and
                row['change'] == qty_ricevuta and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break

    # --- setto la FEE in assets come già elaborata ---
    i_start, i_end = assets.index.slice_locs(start, end)
    for pos in range(i_start, i_end):
        row = assets.iloc[pos]
        if (row['operation'] in ['Fee', 'Transaction Fee'] and
                row['coin'] == fee_coin and
                row['change'] == -qty_fee and
                not row['gia_elaborata']):
            assets.iloc[pos, col_idx] = True
            break
    log_movimento(c_venduta, coin_data, f"SELL vende {c_venduta}", timestamp)
    log_movimento(coin_ricevuta, coin_data, f"SELL riceve {coin_ricevuta}", timestamp)
    log_movimento(fee_coin, coin_data, f"SELL fee {fee_coin}", timestamp)

    if is_fiscal and coin_ricevuta in ['EUR', 'USDC'] and c_venduta != 'EUR':
        # il corrispettivo è il valore in EUR della coin ricevuta
        qty_netta = qty_ricevuta - qty_fee if fee_coin == coin_ricevuta else qty_ricevuta
        if coin_ricevuta == 'EUR':
            corrispettivo_eur = qty_netta if fee_coin == coin_ricevuta else qty_ricevuta
        elif coin_ricevuta == 'USDC' and quotazioni is not None:
            rate = get_price_at_timestamp(quotazioni['USDC-EUR'], pd.to_datetime(timestamp).normalize())
            corrispettivo_eur = (qty_ricevuta - qty_fee if fee_coin == coin_ricevuta else qty_ricevuta) * rate
        else:
            # per altre coin, usa la quotazione EUR se disponibile
            corrispettivo_eur = None  # da valorizzare se hai la quotazione

        costo_fiscale = qty_venduta * pmc_coin_venduta
        plusvalenza = (corrispettivo_eur - costo_fiscale) if corrispettivo_eur is not None else None

        quadro_RT.append({
            'data': timestamp,
            'operazione': 'SELL',
            'coin_ceduta': c_venduta,
            'qty_ceduta': qty_venduta,
            'pmc_coin_ceduta': pmc_coin_venduta,
            'costo_fiscale': costo_fiscale,
            'coin_ricevuta': coin_ricevuta,
            'qty_ricevuta': qty_ricevuta,
            'corrispettivo_eur': corrispettivo_eur,
            'plusvalenza': plusvalenza
        })
    return 0


###################################
## ELABORAZIONE DELLE OPERAZIONI ##
###################################

def process_all_binance_operations(assets, scambi, initial_portfolio, fiscal_start, fiscal_end, eurusd_quotes=None):
    """
    Elabora operazioni con valorizzazione EUR corretta per USDC/USDT
    eurusd_quotes: dict {timestamp: rate} per conversione USD→EUR
    """
    coin_a_pmc_zero = None
    print("\n" + "=" * 80)
    print("ELABORAZIONE OPERAZIONI")
    print("=" * 80 + "\n")
    risposta = input("Contabilizzare il prezzo medio di carico dei soli token crypto al valore di zero? (Y/n):  ")
    while not (risposta == '' or risposta == 'Y' or risposta == 'y' or risposta == 'n' or risposta == 'N'):
          risposta = input ("Valore inserito non accettato, inserire Y o n:  ")
    if risposta == "n" or risposta == "N":
        coin_a_pmc_zero = False
    elif risposta == "y" or risposta == "Y" or risposta == "":
        coin_a_pmc_zero = True



    # DEBUG: Lista per registrare tutte le operazioni
    debug_operations = []

    # Conteggio depositi per logging
    deposits_coinbase = []  # 21-23 aprile 2021
    deposits_other = []  # altri depositi

    coin_data = defaultdict(lambda: {'quantity': 0, 'total_cost': 0, 'Prezzo_Medio_Di_Carico': 0})

    # IMPORTANTE: Inizializza con portfolio Coinbase
    # Questi asset vengono poi trasferiti su Binance tramite depositi 21-23 aprile 2021
    # che verranno SKIPPATI per evitare duplicazione
    # for coin, data in initial_portfolio.items():
    #     coin_data[coin]['quantity'] = data['quantity']
    #     coin_data[coin]['total_cost'] = data['total_cost']

    gains_2025 = []
    rewards_2025 = []

    #ordinavo assets quando era una lista di dizionari
    #assets.sort(key=lambda x: (x['timestamp'], x['change']))  # Negative first, then positive

    #ordinamento assets come Pandas DataFrame
    assets.sort_index(inplace=True)

    fiscal_start_dt = pd.to_datetime(fiscal_start)
    fiscal_end_dt = pd.to_datetime(fiscal_end)

    for pos, (i, op) in enumerate(assets.iterrows()):
        timestamp = i

        # legge il valore LIVE dal DataFrame con posizione intera (sempre scalare)
        if assets.iloc[pos]['gia_elaborata']:
            continue

        is_fiscal = fiscal_start_dt <= timestamp <= fiscal_end_dt
        op_type = op['operation']
        coin = op['coin']
        change = op['change']

        if (coin == 'ETH' or coin == 'BETH'):
            if any(keyword in op_type for keyword in ['Liquid', 'Liquidity', 'Swap Farming']):
                continue

        qty_before = coin_data[coin]['quantity']
        cost_before = coin_data[coin]['total_cost']

        if op_type == 'Deposit':
            deposita_coin(coin, coin_data, change, timestamp, coin_a_pmc_zero)
        elif op_type == 'Withdraw':
            preleva_coin(coin, coin_data, change, timestamp)
        elif op_type in ['Buy', 'Sell', 'Transaction Buy','Transaction Revenue', 'Transaction Spend', 'Transaction Sold']:
            start = timestamp - timedelta(minutes=2)
            end = timestamp + timedelta(minutes=2)

            risultati = scambi[
                (scambi['timestamp'].between(start, end)) &
                (scambi['gia_elaborata'] == False) &
                (((scambi['coin'] == coin) &
                  (abs(scambi['change']) == abs(change))) |
                 ((scambi['quote_coin'] == coin) &
                  (abs(scambi['quote_amount']) == abs(change))))
                ]

            if len(risultati) == 0:
                print(f"Nessuno scambio trovato nel periodo {timestamp}, con la coin {coin} di importo {change}:")
                raise Exception("Mi fermo perché non ho trovato uno scambio al quale associare l'operazione")
            if len(risultati) > 1:
                print(f"Trovate più scambi nel periodo {timestamp}, con la coin {coin} di importo {change}:")
                print(risultati.to_string())


            scambio = risultati.iloc[0]

            if scambio['operation'] == 'BUY':
                # l'asset corrente è la coin ACQUISTATA → elabora_buy
                elabora_buy(scambio, coin_data, timestamp, assets, i, quadro_RT, is_fiscal)

            elif scambio['operation'] == 'SELL':

                # l'asset corrente è la coin VENDUTA → elabora_sell
                elabora_sell(scambio, coin_data, timestamp, assets, i, quadro_RT, is_fiscal)

            else:
                raise Exception(f"Trovato uno scambio che non è nè BUY, nè SELL, è {scambio['operation']}")
        elif op_type == 'Binance Convert':
            elabora_binance_convert(coin, change, timestamp, assets, coin_data, quadro_RT, is_fiscal)
        elif op_type in ['Simple Earn Locked Rewards', 'Simple Earn Flexible Interest', 'Staking Rewards',
                         'ETH 2.0 Staking Rewards', 'Swap Farming Rewards', ]:
            elabora_reward(coin, change, timestamp, coin_data, assets, op_type, quadro_RT, is_fiscal)
        elif op_type in ['HODLer Airdrops Distribution', 'Launchpool Airdrop - User Claim Distribution',
        'Launchpad Token Distribution', 'Launchpool Airdrop - System Distribution', 'Megadrop Rewards']:
            elabora_airdrop(coin, change, timestamp, coin_data, assets, op_type, quadro_RT, is_fiscal)

    return coin_data, quadro_RT


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #print("Percorso corrente:", os.getcwd())
    quotazioni = load_quotes()
    # print(quotazioni['USDC-EUR'][:])
    start_dt = pd.to_datetime(START_DATE)
    end_dt = pd.to_datetime(END_DATE)
    assets = load_asset(start_dt, end_dt)

    print(assets.columns.tolist())
    #assets è un dataFrame che è originato da una lista [] dove ogni elemento è un dizionario di questo tipo:
    # {
    # 'timestamp': timestamp,
    # 'operation': operation,
    # 'coin': coin,
    # 'change': change,
    # 'remark': row.get('Remark', ''),
    # 'source': file,
    # 'gia_elaborata': False
    # }
    # Quando una coin viene processata, gia_elaborata diventa True



    # Controllo se le prime 10 operazioni corrispondono
    # print("Stampo prime 10 operazioni")
    # print(assets[['timestamp', 'operation', 'change', 'remark', 'gia_elaborata']].head(10).to_string(index=False))
    # print(len(assets))
    #
    # # Filtro per BNB
    # bnb_ops = assets[assets['coin'] == 'BNB']
    #
    # print("\n--- Le prime 10 operazioni BNB ---")
    # print(bnb_ops[['timestamp', 'operation', 'change', 'remark']].head(10).to_string(index=False))
    #
    # # Calcolo il bilancio totale netto di BNB
    # bilancio_totale = bnb_ops['change'].sum()
    # print(f"\nBilancio finale BNB nel periodo: {bilancio_totale:.8f}")
    # scambi = load_scambi(BINANCE_BASE_DIR)
    # df_scambi = pd.DataFrame(scambi)
    # print(df_scambi[df_scambi["coin"] =="BTC"].to_string())
# print(isinstance(operazioni, list))
# print(quotazioni['USDC-EUR']["2025-12-27"])
    scambi = load_scambi(BINANCE_BASE_DIR)
    scambi = pd.DataFrame(scambi)
    # dopo il caricamento, cerca quella riga specifica

#     prova = [ {'timestamp': pd.to_datetime('2021-04-21 18:55:00'), 'operation': 'Deposit', 'coin': 'USDT', 'change': 1000, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv', 'gia_elaborata': False},
#               {'timestamp': pd.to_datetime('2021-09-29 12:56:00'), 'operation': 'Deposit', 'coin': 'POL', 'change': 100, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv', 'gia_elaborata': False},
#               #caso reale acquisto POL usando USDT pagando fee in POL
#              {'timestamp': pd.to_datetime('2024-09-29 12:56:00'), 'operation': 'Buy', 'coin': 'POL', 'change': 108.3, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv', 'gia_elaborata': False},
#               {'timestamp': pd.to_datetime('2024-09-29 12:56:00'), 'operation': 'Fee', 'coin': 'POL', 'change': -0.1083, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv', 'gia_elaborata': False},
#               {'timestamp': pd.to_datetime('2024-09-29 12:56:00'), 'operation': 'Sell', 'coin': 'USDT', 'change': -45.07446, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv', 'gia_elaborata': False},
#               #caso reale acquisto ETH usando EUR e pagando fee in BNB
#               {'timestamp': pd.to_datetime('2022-04-14 20:50:01'), 'operation': 'Transaction Buy', 'coin': 'ETH','change': 0.0181, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv','gia_elaborata': False},
#               {'timestamp': pd.to_datetime('2022-04-14 20:50:01'), 'operation': 'Transaction Spend', 'coin': 'EUR','change': -50.41936, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv','gia_elaborata': False},
#               {'timestamp': pd.to_datetime('2022-04-14 20:50:01'), 'operation': 'Transaction Fee', 'coin': 'BNB','change': -0.00009858, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv','gia_elaborata': False},
#              {'timestamp': pd.to_datetime('2025-04-21 18:55:00'), 'operation': 'Deposit', 'coin': 'USDC', 'change': 1000, 'remark': None, 'source': 'D:/730/2026/binance/asset\\1-1-2017--31-12-2025.csv', 'gia_elaborata': False}]
#     df_prova = pd.DataFrame(prova).set_index('timestamp').sort_index()
# #    coin_data = process_all_binance_operations(df_prova, scambi, None, start_dt, end_dt, quotazioni)
    coin_data, quadro_RT = process_all_binance_operations(assets, scambi, None, FISCAL_YEAR_START, FISCAL_YEAR_END,
                                                          quotazioni)

    print(coin_data)

    # salva quadro_RT in CSV
    anno_fiscale = pd.to_datetime(FISCAL_YEAR_START).year

    if quadro_RT:
        nome_file_rt = os.path.join(BINANCE_BASE_DIR, f"QuadroRT_Binance_{anno_fiscale}.csv")
        df_quadro_RT = pd.DataFrame(quadro_RT)
        df_quadro_RT.to_csv(nome_file_rt, index=False, sep=';', decimal=',', encoding='utf-8-sig')
        print(f"\nQuadro RT salvato in: {nome_file_rt}")
        print(f"Righe totali: {len(df_quadro_RT)}")
        plusvalenza_totale = df_quadro_RT['plusvalenza'].dropna().sum()
        print(f"Plusvalenza totale: {plusvalenza_totale:.2f} EUR")
    else:
        print("\nNessuna operazione fiscalmente rilevante trovata per il quadro RT.")

    # salva coin_data in CSV
    nome_file_portfolio = os.path.join(BINANCE_BASE_DIR, f"Portfolio_Binance_{anno_fiscale}.csv")
    df_portfolio = pd.DataFrame([
        {
            'coin': coin,
            'quantity': data['quantity'],
            'total_cost': data['total_cost'],
            'Prezzo_Medio_Di_Carico': data['Prezzo_Medio_Di_Carico']
        }
        for coin, data in coin_data.items()
        if data['quantity'] != 0  # esclude coin con quantità zero
    ]).sort_values('total_cost', ascending=False)

    df_portfolio.to_csv(nome_file_portfolio, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    print(f"\nPortfolio salvato in: {nome_file_portfolio}")
    print(f"Coin in portafoglio: {len(df_portfolio)}")
    print(df_portfolio.to_string(index=False))

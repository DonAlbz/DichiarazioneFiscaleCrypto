from dis import code_info

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
# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
COINBASE_INITIAL_FILE = 'D:/730/2026/coinbase_initial_simple.csv'
BINANCE_BASE_DIR = 'D:/730/2026/binance'
BINANCE_ASSET_MASTER = 'D:/730/2026/binance/asset/1-1-2017--31-12-2025.csv'

START_DATE = "2021-01-01"
END_DATE = "2025-12-31 23:59:59"
FISCAL_YEAR_START = "2025-01-01"
FISCAL_YEAR_END = "2025-12-31 23:59:59"


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


        df = pd.read_csv(file)

        # Skippa la seconda riga se contiene i ticker ripetuti
        # Identifica se la prima riga di dati contiene il ticker
        # non dovrebbe servire per gli asset, ma meglio controllare
        if len(df) > 0 and isinstance(df.iloc[0]['UTC_Time'], str) and not df.iloc[0]['UTC_Time'].replace('-',
                                                                                                  '').isdigit():
            df = df.iloc[1:].reset_index(drop=True)  # droppo la seconda riga se ticker

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

            change = float(row['Change'])
            operation = row['Operation']

           # Aggiungo l'operazione
            operations.append({
                'timestamp': timestamp,
                'operation': operation,
                'coin': coin,
                'change': change,
                'remark': row.get('Remark', ''),
                'source': file
            })

            new_operations[operation] += 1


        # print(f"   Operazioni uniche da aggiungere: {len(operations)}")

        if len(operations) > 0:
            print(f"\n      Nuove operazioni per tipo (top 15):")
            sorted_ops = sorted(new_operations.items(), key=lambda x: -x[1])
            for op, count in sorted_ops[:15]:
                print(f"      {op:<50} {count:>5}x")
        print()
        return operations

    except Exception as e:
        print("!"*80)
        print(f"Errore caricamento CSV master: {e}\n")
        print("!" * 80)
        import traceback
        traceback.print_exc()
        return []




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
            df = pd.read_csv(file)
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
                            # IMPORTANTE: Aggiungi fingerprint multipli per deduplicazione con master
                            'master_fingerprints': [
                                # 1. Buy coin (executed) - positivo
                                f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{executed_coin}|{executed_val:.8f}",
                                # 2. Sell quote_coin (amount) - negativo nel master
                                f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{amount_coin}|{-amount_val:.8f}",
                                # 3. Fee - negativo
                                f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{fee_coin}|{-fee_val:.8f}" if fee_val > 0 else None
                            ]
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
                            # IMPORTANTE: Fingerprint multipli
                            'master_fingerprints': [
                                # 1. Sell coin (executed) - negativo
                                f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{executed_coin}|{-executed_val:.8f}",
                                # 2. Buy quote_coin (amount) - positivo nel master
                                f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{amount_coin}|{amount_val:.8f}",
                                # 3. Fee - negativo
                                f"{timestamp.strftime('%Y-%m-%d %H:%M')}|{fee_coin}|{-fee_val:.8f}" if fee_val > 0 else None
                            ]
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #print("Percorso corrente:", os.getcwd())
    quotazioni = load_quotes()
    # print(quotazioni['USDC-EUR'][:])
    start_dt = pd.to_datetime(START_DATE)
    end_dt = pd.to_datetime(END_DATE)
    operazioni = load_asset(start_dt, end_dt)
    if operazioni:
        # Converto la lista di dizionari in un DataFrame
        df_ops = pd.DataFrame(operazioni)
    load_scambi(BINANCE_BASE_DIR)
        # Controllo se le prime 10 operazioni corrispondono
        # print("Stampo prime 10 operazioni")
        # print(df_ops[['timestamp', 'operation', 'change', 'remark']].head(10).to_string(index=False))
        # print(len(df_ops))
        #
        # # Filtro per BNB
        # bnb_ops = df_ops[df_ops['coin'] == 'BNB']
        #
        # print("\n--- Le prime 10 operazioni BNB ---")
        # print(bnb_ops[['timestamp', 'operation', 'change', 'remark']].head(10).to_string(index=False))
        #
        # # Calcolo il bilancio totale netto di BNB
        # bilancio_totale = bnb_ops['change'].sum()
        # print(f"\nBilancio finale BNB nel periodo: {bilancio_totale:.8f}")





import pandas as pd
import math
import time
import os
from binance.spot import Spot as Client
from datetime import datetime, timedelta, timezone

API_KEY = ''
API_SECRET = ''
client = Client(API_KEY, API_SECRET)


def get_historical_data(symbol, interval, start_date, end_date):


    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    print("Start datetime:", start_dt, "| Start timestamp (ms):", start_ts)
    print("End datetime:", end_dt, "| End timestamp (ms):", end_ts)

 
    # Adatok tárolására
    all_data = []

    # Kezdő időpont beállítása
    current_ts = start_ts

    # 🔹 3. Ciklus, amíg el nem érjük a végdátumot
    while current_ts < end_ts:
        print(f"Lekérdezés {datetime.fromtimestamp(current_ts / 1000, tz=timezone.utc)} -ig...")

        # API hívás
        data = client.klines(
            symbol=symbol,
            interval=interval,
            startTime=current_ts,
            endTime=end_ts,  # A végdátum marad fix
            limit=1000  # Binance korlát (max 1000 adat egyszerre)
        )

        # Ha nincs több adat, kilépünk
        if not data:
            print("Nincs több elérhető adat.")
            break

        # Adatok hozzáadása
        all_data.extend(data)

        # Utolsó timestamp frissítése az új lekérésekhez
        last_ts = data[-1][0]  # Utolsó gyertya kezdő időpontja
        current_ts = last_ts + (5 * 60 * 1000)  # Következő gyertya kezdő ideje

        time.sleep(1)  # Várakozás, hogy ne legyünk túlterhelve

    # 🔹 4. Pandas DataFrame létrehozása
    df = pd.DataFrame(all_data, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ])

    # Timestamp átalakítása olvasható dátummá (Budapest időzónára konvertálva)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')

    # Csak a fontos oszlopokat tartsuk meg
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df.rename(columns={"timestamp": "date"}, inplace=True)

    # Ellenőrzés - első pár sor kiíratása
    return(df)





ticker = 'BTCUSDT'


def get_one_ticker(ticker):
    base_folder = f'binance_data/all_timeframe/{ticker}/'
    start_date = '2021-11-10'
    end_date = '2025-02-10'
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    for timeframe in [ '5m', '15m', '1h', '4h', '1d']:
        print(f"Adatok lekérése {timeframe} időkeretre..., {start_date} - {end_date}")
        df = get_historical_data('BTCUSDT', timeframe, start_date=start_date, end_date=end_date)
        df.to_pickle(f"{base_folder}/{ticker}_binance_{timeframe}_data.pkl")

    print(f"Adatok elmentve: {base_folder}/{ticker}_binance_{timeframe}_data.pkl")



symbols = ['BNBUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT',  'LINKUSDT', 'ADAUSDT']

for ticker in symbols:
    print(f"Adatok lekérése a következő tickerre: {ticker}")
    print('----------------------------------------------\n\n') 
    get_one_ticker(ticker)

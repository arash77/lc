from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.command import Command
import time
import pandas as pd
import os
import ccxt
import numpy as np

import redis
import pickle
r = redis.Redis(host='localhost', port=6379, db=0)

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_experimental_option("detach", True)
d = webdriver.Chrome(chrome_options=chrome_options)

while True:
    try:
        d.execute(Command.STATUS)
    except Exception as e:
        print(e, time.ctime())
        d = webdriver.Chrome(chrome_options=chrome_options)
    d.get("https://lunarcrush.com/markets?tv=gt_500k&rpp=500&ob=bullish_sentiment")
    time.sleep(5)
    check = WebDriverWait(d, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'MuiTableRow-root'))).text.splitlines()
    print(check, time.ctime())
    if 'Average Sentiment' not in check:
        d.find_elements_by_class_name("MuiButtonBase-root")[9].click()
        WebDriverWait(d, 30).until(EC.presence_of_element_located((By.ID, 'email')))
        d.find_element_by_id("email").send_keys("arash77.kad@gmail.com")
        d.find_element_by_id("password").send_keys("arash1377")
        d.find_element_by_xpath("//button[@type='submit']").click()
        WebDriverWait(d, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'MuiAvatar-root')))
        time.sleep(5)
    else:
        theList = WebDriverWait(d, 300).until(EC.presence_of_element_located((By.CLASS_NAME, 'MuiTableBody-root'))).text.splitlines()
        d.quit()
        if 'COIN OF THE DAY' in theList: theList.remove('COIN OF THE DAY')
        data_of_web = list()
        list_of_coins = [theList[n:n+7] for n in range(0, len(theList), 7)]
        print(list_of_coins[-1], time.ctime())
        try:
            for i in list_of_coins:
                if 'N/A' in i[6].split()[9]:
                    z = i[6].split()
                    z[9] = "NA NA"
                    i[6] = " ".join(z)
                if int(i[6].split()[17].replace(',', '')) > 1 and int(i[6].split()[16].replace(',', '')) > 1:
                    data_of_web.append(dict(
                        coin = i[2],
                        galaxy_score = float(i[4]),
                        alt_rank = int(i[5].split()[0].replace(',', '')),
                        social_score = int(i[6].split()[12].replace(',', '')),
                        social_contributors = int(i[6].split()[13].replace(',', '')),
                        average_sentiment = float(i[6].split()[15].split("%")[0].replace(',', '')),
                        BullBear = int(i[6].split()[16].replace(',', ''))/int(i[6].split()[17].replace(',', '')),))
        except Exception as e:
            print(e, time.ctime())
        df_new = pd.DataFrame(data_of_web)
        if 'data.csv' in os.listdir(os.getcwd()):
            df_old = pd.read_csv('data.csv')
            df_old = df_old.drop(['social_score_old'], axis='columns', errors='ignore')
            df_old = df_old.rename({'social_score': 'social_score_old'}, axis='columns')
            df_old = df_old.drop(['ID_old'], axis='columns', errors='ignore')
            df_old = df_old.rename({'ID': 'ID_old'}, axis='columns')
            df_new = df_new.set_index(['coin']).combine_first(df_old.set_index(['coin'])).reset_index()
            
        Markets = dict()
        for exchange in ['coinex','binance','huobipro']:
            try:
                Markets[exchange] = [market['base'] for market in getattr(ccxt, exchange)({'timeout': 5000,'enableRateLimit': True}).fetchMarkets() if market['quote'] == "USDT"]
            except Exception as e:
                print(e, time.ctime())

        df_new['exchange'] = df_new['coin'].apply(lambda row: ",".join([exch for exch, j in Markets.items() if row in j]))
        df_new['exchange'].replace('', np.nan, inplace=True)
        df_new = df_new.dropna(subset=['exchange'])

        df = df_new.reset_index(drop=True)
        if 'social_score_old' in df.columns:
            df['social_score_change'] = df['social_score'] - df['social_score_old']
            if 'social_score_change_percent' in df.columns:
                notnulls = df['social_score_change_percent'].notnull()
                df.loc[notnulls, 'social_score_change_percent'] = ((df.loc[notnulls, 'social_score_change'] * 100 / df.loc[notnulls, 'social_score_old']) + df.loc[notnulls, 'social_score_change_percent']) / 2
                isnulls = df['social_score_change_percent'].isnull()
                df.loc[isnulls, 'social_score_change_percent'] = (df.loc[isnulls, 'social_score_change'] * 100 / df.loc[isnulls, 'social_score_old'])
            else:
                df['social_score_change_percent'] = df['social_score_change'] * 100 / df['social_score_old']
        df = df.assign(rate=0)
        for rank, asc in [["galaxy_score",False],["BullBear",False]]:
            df = df.sort_values(rank,ascending=asc,ignore_index=True)
            for i in range(len(df)):
                df.loc[i, 'rate'] += (len(df)-i)
        df = df.sort_values("rate",ascending=False,ignore_index=True)
        df.insert(0, 'ID', range(1, 1+len(df)))
        if 'ID_change' in df.columns:
            df = df.rename({'ID_change': 'ID_2change'}, axis='columns')
        if 'ID_old' in df.columns:
            df['ID_change'] = df['ID_old'] - df['ID']
        
        print(df.head(30), time.ctime())
        df.to_csv('data.csv',index=False)
        r.set('records',pickle.dumps(df[['coin', 'BullBear', 'exchange', 'galaxy_score', 'rate', 'social_score']].to_dict('records')))
        # pickle.loads(r.get('records'))
        time.sleep(60)
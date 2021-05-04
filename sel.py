from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import pandas as pd
import os
import ccxt

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
d = webdriver.Chrome(chrome_options=chrome_options)
d.get("https://lunarcrush.com/markets?rpp=10")
print(WebDriverWait(d, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'MuiTableRow-root'))).text.splitlines())

if 'Average Sentiment' not in d.find_elements_by_class_name("MuiTableRow-root")[0].text.splitlines():
    d.find_elements_by_class_name("MuiButtonBase-root")[9].click()
    WebDriverWait(d, 30).until(EC.presence_of_element_located((By.ID, 'email')))
    d.find_element_by_id("email").send_keys("arash77.kad@gmail.com")
    d.find_element_by_id("password").send_keys("arash1377")
    d.find_element_by_xpath("//button[@type='submit']").click()
    time.sleep(10)
    d.get("https://lunarcrush.com/markets?tv=gt_500k&rpp=500&ob=bullish_sentiment")
    time.sleep(1)
    print(d.find_elements_by_class_name("MuiTableRow-root")[0].text.splitlines())
if 'Average Sentiment' in d.find_elements_by_class_name("MuiTableRow-root")[0].text.splitlines():
    data_of_web = list()
    theList = WebDriverWait(d, 200).until(EC.presence_of_element_located((By.CLASS_NAME, 'MuiTableBody-root'))).text.splitlines()
    d.close()
    d.quit()
    if 'COIN OF THE DAY' in theList:
        theList.remove('COIN OF THE DAY')
    df = pd.DataFrame([theList[n:n+7] for n in range(0, len(theList), 7)])
    df[6] = df[6].str.replace("N/A","Na Na")
    df[6].str.split(' ',expand=True)
    for i in [theList[n:n+7] for n in range(0, len(theList), 7)]:
        try:
            if 'N/A' in i[6].split()[9]:
                z = i[6].split()
                z[9] = "Na Na"
                i[6] = " ".join(z)
            if int(i[6].split()[17].replace(',', '')) > 1 and int(i[6].split()[16].replace(',', '')) > 1:
                data_of_web.append(
                    dict(
                        coin = i[2],
                        galaxy_score = float(i[4]),
                        alt_rank = int(i[5].split()[0].replace(',', '')),
                        social_score = int(i[6].split()[12].replace(',', '')),
                        social_contributors = int(i[6].split()[13].replace(',', '')),
                        average_sentiment = float(i[6].split()[15].split("%")[0].replace(',', '')),
                        BullBear = int(i[6].split()[16].replace(',', ''))/int(i[6].split()[17].replace(',', '')),
                    )
                )
        except Exception as e:
            print(e)
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
            markets_tmp = getattr(ccxt, exchange)({'timeout': 5000,'enableRateLimit': True}).fetchMarkets()
            coins_tmp = list()
            for market in markets_tmp:
                if market['quote'] == "USDT":
                    coins_tmp.append(market['base'])
            Markets[exchange] = coins_tmp
        except:
            print("ERR NO TICKER", time.ctime())
    df_new = df_new.assign(exchange=None)
    for i in range(len(df_new)):
        exch_tmp=list()
        for exch, j in Markets.items():
            if df_new.loc[i, 'coin'] in j: exch_tmp.append(exch)
        df_new.loc[i, 'exchange'] = ",".join(exch_tmp)
    df_new.dropna(subset=['exchange'],inplace=True)
    
    df_new.to_csv('data.csv',index=False)
    print(df_new)


if 'data.csv' in os.listdir(os.getcwd()):
    df = pd.read_csv('data.csv')

    df = df.assign(social_score_change=0)
    if 'social_score_old' in df.columns:
        for i in range(len(df)):
            if df.loc[i, 'social_score_old']!=0:
                df.loc[i, 'social_score_change'] = (df.loc[i, 'social_score'] - df.loc[i, 'social_score_old'])
                df.loc[i, 'social_score_change_percent'] = df.loc[i, 'social_score_change'] * 100 / df.loc[i, 'social_score_old']
    df = df.assign(rate=0)
    for rank, asc in [["galaxy_score",False],["average_sentiment",False],["BullBear",False]]:
        df = df.sort_values(rank,ascending=asc,ignore_index=True)
        for i in range(len(df)):
            df.loc[i, 'rate'] += (len(df)-i)
    df = df.sort_values("rate",ascending=False,ignore_index=True)
    df.insert(0, 'ID', range(1, 1+len(df)))
    if 'ID_old' in df.columns:
        for i in range(len(df)):
            df.loc[i, 'ID_2change'] = df.loc[i, 'ID_change']
            df.loc[i, 'ID_change'] = (df.loc[i, 'ID_old'] - df.loc[i, 'ID'])
    df = df.assign(new_rate=0)
    if 'social_score_change_percent' in df.columns and 'ID_change' in df.columns:
        for rank, asc in [["social_score_change_percent",False],["ID_change",False]]:
            df = df.sort_values(rank,ascending=asc,ignore_index=True)
            for i in range(len(df)):
                df.loc[i, 'new_rate'] += (len(df)-i)
        df = df.sort_values("new_rate",ascending=False,ignore_index=True)
    
    print(df.head(30))
    df.to_csv('data.csv',index=False)
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import pandas as pd
from pandas.tseries.frequencies import to_offset
import os


def connect():
    password = "jw8s0F4"
    global engine
    engine = create_engine('postgresql://manuel:{}@45.79.192.187:5432/sterlingsquare_db'.format(password))
    global sql
    sql = engine.connect()
    global df
    df = pd.read_sql_table("accounts_stocknames", con=engine)
    # df['portfolio_id_id']=df['portfolio_id_id'].fillna('0.2')
    df.drop_duplicates(inplace=True)
    #     if not os.path.isfile('calculated_monthy.csv'):
    #                            df.to_csv('dashboard_api_portfolioperformance.csv',header='column_names')
    #                     else: # else it exists so append without writing the header
    #                         nam_new.to_csv('calculated_monthy.csv', mode='a', header=False)
    df.to_csv('accounts_stocknames.csv', header='column_names')
    #     global sam
    #     sam=df.groupby('user_id_id')
    #     global sam1
    #     sam1=df.groupby('user_id_id')
    # #     sql = text('DROP TABLE IF EXISTS daily_return;')
    #     result = engine.execute(sql)

    return print("connection success")


import requests
import re
import pymongo
from bs4 import BeautifulSoup
import time
from sqlalchemy import insert

from datetime import datetime
from sqlalchemy import MetaData, Table


# datetime object containing current date and time


def google_news():
    connect()
    now = datetime.now()
    meta = MetaData(engine)
    password = "jw8s0F4"
    accounts_googlenews = Table('accounts_googlenews', meta)
    engine1 = create_engine('postgresql://manuel:{}@45.79.192.187:5432/sterlingsquare_db'.format(password))
    # sql
    sql1 = engine.connect()

    # myclient = pymongo.MongoClient("mongodb://50.116.32.224:27017/")

    # mydb = myclient["news&tweets"]

    # mycol = mydb["news"]

    googleTrendsUrl = 'https://google.com'
    response = requests.get(googleTrendsUrl)
    if response.status_code == 200:
        g_cookies = response.cookies.get_dict()
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)\
                AppleWebKit/537.36 (KHTML, like Gecko) Cafari/537.36'}

    for i in range(len(df)):
        url = 'https://news.google.com/search?q=' + df['name'].values[i]
        page = requests.get(url, headers=headers, cookies=g_cookies)
        soup = BeautifulSoup(page.content, 'html.parser')
        remove = "Google News - SearchNewsSign inNewsTop storiesFor youFollowingNews ShowcaseSaved searchesCOVID-19IndiaWorldYour local newsBusinessTechnologyEntertainmentSportsScienceHealthLanguage & regionEnglish (India)SettingsGet the Android appGet the iOS appSend feedbackHelpPrivacy · Terms · About"
        sam = soup.text
        sam_1 = sam.strip(remove)
        print(sam_1)
        df_new = {'created': now, 'heading': df['name'].values[i],
                  'types': "", 'redirection_link': '', 'description': sam_1, 'sentiment': "",
                  'symbol_id': int(df['id'].values[i]), 'tags': ''}
        sample = pd.DataFrame([df_new])

        sample.to_sql('accounts_googlenews', con=engine1, if_exists='append', index=False)
        print(sample)

#         statement = accounts_googlenews.insert().values({'created':now,'heading':df['name'].values[i],
#                                                        'types':"",'redirection_link':'','description':sam_1,'sentiment':"",
#                                                        'symbol_id':df['symbol'].values[i],'tags':''})
#         sql.execute(statement)

# my_d = {"stock_name":i,"google_news":sam_1}

# x = mycol.insert_one(my_d)


google_news()
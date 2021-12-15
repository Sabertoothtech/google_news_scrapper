import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import pandas as pd
from pandas.tseries.frequencies import to_offset
import os

import requests
import re
import pymongo
from bs4 import BeautifulSoup



def connect():
    password = "jw8s0F4"
    global engine
    engine = create_engine('postgresql://manuel:{}@50.116.32.224:5432/pradeep_test'.format(password))
    sql = engine.connect()
    global df
    df = pd.read_sql_table("accounts_stocknames", con=engine)
    # df['portfolio_id_id']=df['portfolio_id_id'].fillna('0.2')
    df.drop_duplicates(inplace=True)
    #     if not os.path.isfile('calculated_monthy.csv'):
    #                            df.to_csv('dashboard_api_portfolioperformance.csv',header='column_names')
    #                     else: # else it exists so append without writing the header
    #                         nam_new.to_csv('calculated_monthy.csv', mode='a', header=False)
    #df.to_csv('accounts_stocknames.csv', header='column_names')
    #     global sam
    #     sam=df.groupby('user_id_id')
    #     global sam1
    #     sam1=df.groupby('user_id_id')
    # #     sql = text('DROP TABLE IF EXISTS daily_return;')
    #     result = engine.execute(sql)

    return print("connection success")

def g_cook():
    googleTrendsUrl = 'https://google.com'
    response = requests.get(googleTrendsUrl)
    if response.status_code == 200:
        global g_cookies
        g_cookies = response.cookies.get_dict()


def google_news():
    connect()

    myclient = pymongo.MongoClient("mongodb://50.116.32.224:27017/")

    mydb = myclient["news&tweets"]

    mycol = mydb["news"]

    #g_cook()

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)\
                AppleWebKit/537.36 (KHTML, like Gecko) Cafari/537.36'}

    for i in df['name'].values:
        url = 'https://news.google.com/search?q=' + i
        page = requests.get(url, headers=headers)
        #cookies=g_cookies)
        soup = BeautifulSoup(page.content, 'html.parser')
        remove = "Google News - SearchNewsSign inNewsTop storiesFor youFollowingNews ShowcaseSaved searchesCOVID-19IndiaWorldYour local newsBusinessTechnologyEntertainmentSportsScienceHealthLanguage & regionEnglish (India)SettingsGet the Android appGet the iOS appSend feedbackHelpPrivacy · Terms · About"
        sam = soup.text
        sam_1 = sam.strip(remove)
        print(sam_1)
        my_d = {"stock_name": i, "google_news": sam_1}
        x = mycol.insert_one(my_d)
        return print(x)


google_news()
# import schedule
# schedule.every(2).seconds.do(google_news)
#
# import time
# while True:
#     schedule.run_pending()
#     time.sleep(4)
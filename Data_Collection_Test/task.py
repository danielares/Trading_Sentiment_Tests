import requests
from datetime import datetime, timedelta
import time

import pandas as pd


DAYS = timedelta(29)
BASE_URL = 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=500'


def get_data_range():

    #Today:
    end_time = datetime.now()
    
    #Today - 30 days:
    start_time = end_time - DAYS

    # Convert to Unix Timestamp - ms
    end_time_stamp = int(time.mktime(end_time.timetuple()) * 1000)
    start_time_stamp = int(time.mktime(start_time.timetuple()) * 1000)

    return end_time_stamp, start_time_stamp


def get_binance_data(start_time, end_time):
    url = BASE_URL
    url += (f"&startTime={start_time}")
    url += (f"&endTime={end_time}")
    data = requests.get(url)
    df = pd.json_normalize(data.json())
    print(type(df))
    return df


end_time_stamp, start_time_stamp = get_data_range()

df_final = pd.DataFrame()

while ( end_time_stamp >= start_time_stamp ):
    df = get_binance_data(start_time_stamp, end_time_stamp )
    df = df[::-1] #reverse rows of df
    df_final = pd.concat([df_final, df])
    
    # 5 minutes = 300000 Unix Timestamp 
    # 300000 * 500 (requisitions limit) = 150000000
    end_time_stamp -= 150000000 

# deleting columns that were not requested in the task
df_final=df_final.drop(["longAccount", "shortAccount"],axis=1)

# CSV
df_final.to_csv('output.csv', encoding='utf-8', index=False)

print(df_final.columns)
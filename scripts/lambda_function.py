import json
import requests
import pandas as pd
import awswrangler as wr
from datetime import datetime

def lambda_handler(event,context):
    now = datetime.now()
    #time_period_from = '2023-08-01T00:00Z'
    time_period_from  = event["time_from"]
    time_period_to = event["time_to"]
    url = 'https://api.carbonintensity.org.uk/generation/' + time_period_from +'/' + time_period_to
    # https://carbon-intensity.github.io/api-definitions/?python#carbon-intensity-api-v2-0-0
    headers = {
      'Accept': 'application/json'
    }
    r = requests.get(url, params={}, headers = headers)
    raw = r.json()
    raw = raw['data']
    df = pd.json_normalize(raw, 'generationmix',['from','to'])
    file_path = "s3://greenbucketdata/Data/"+ now.strftime("%Y-%m-%d-%H")+".csv"
    wr.s3.to_csv(df,file_path, index=False)

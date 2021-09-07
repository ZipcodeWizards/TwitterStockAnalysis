import requests
import pandas as pd
from sean_keys import access_key

params = {
  'access_key': access_key
}

api_result = requests.get('https://api.marketstack.com/v1/intraday?api_key=21ec92a9b6daeda6854d1dddb77fade0&symbols=TSLA&interval=30min',params)

api_response = api_result.json()
#for dic in api_response['data']:
 #   print(dic['date'], dic['last'])
df = pd.DataFrame(api_response['data'])
#print(api_response)
print(df)
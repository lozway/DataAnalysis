import requests
import json
from datetime import datetime
import os
API_KEY = 'b563eb5f4909061c12dba52f882c4260'
USER_AGENT = 'Dataquest'

def lastfm_get(payload):
    # define headers and URL
    headers = {'user-agent': USER_AGENT}
    url = 'https://ws.audioscrobbler.com/2.0/'

    # Add API key and format to the payload
    payload['api_key'] = API_KEY
    payload['format'] = 'json'
    payload['limit'] = 20

    response = requests.get(url, headers=headers, params=payload)
    return response

def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

r = lastfm_get({'method': 'chart.gettopartists'})
r.status_code
data = r.json()
folder = "./datalake_bronze"
if not os.path.exists(folder):
    os.makedirs(folder)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"lastfm_music_{timestamp}.json"
filepath = os.path.join(folder, filename)

with open(filepath, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"Se han guardado 20 registros en: {filepath}")
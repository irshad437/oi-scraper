# Script to fetch nifty spot open price
import json
import requests
import sys

INDICES_URL = 'https://www1.nseindia.com/homepage/Indices1.json'

key_names = {
    'nifty': 'NIFTY 50 Pre Open',
    'nifty_bank': 'NIFTY BANK'
}


def get_url(url):
    try:
        headers = {'Accept': '*/*',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
                   'X-Requested-With': 'XMLHttpRequest'
                   }
        response = requests.get(url, headers=headers)
        # print('response from NSE', response)
        print(response.status_code)
        content = response.content.decode("utf8")
        # print(content)
        if response.status_code != 200:
            raise Exception(content)
        return content
    except Exception as e:
        print('Exception occured while trying to fetch data from NSE.')
        print(e)
        print('Terminating...')
        sys.exit()


indices_data = json.loads(get_url(INDICES_URL))
# NIFTY 50
# NIFTY 50 Pre Open
# NIFTY BANK
prices = {}
prices['nifty'] = float((next((x for x in indices_data['data'] if x['name']
                               == 'NIFTY 50 Pre Open'), None))['lastPrice'].replace(',', ''))
prices['nifty_bank'] = float((next((x for x in indices_data['data'] if x['name'] == 'NIFTY BANK'), None))[
                             'lastPrice'].replace(',', ''))


print(prices)
with open('./prices.json', 'w') as outfile:
    json.dump(prices, outfile)

# -*- coding: utf-8 -*-

# send message on telegram
import json 
import requests
import config

URL = "https://api.telegram.org/bot{}/".format(config.TELEGRAM_TOKEN)


def get_url(url):
    response = requests.get(url)
    content = response.content.decode("utf8")
    return content


def send_telegram(text, chat_id):
    if chat_id:
        url = URL + "sendMessage?text={}&chat_id={}".format(text, chat_id)
        get_url(url)

# send_telegram('Hey there123 DS21', TELEGRAM_CHAT_ID)
# send message on telegram ends

# scrape option chain data
import requests
import pandas as pd
import re
import time
import math

from bs4 import BeautifulSoup

def findNiftyRange(spot):
    spotRound = roundIt(spot, 50)
    if spot % 100 == 0:
        return False
    elif spotRound % 100 == 0 and spot < spotRound:
        return [spotRound - 100, spotRound - 50, spotRound]
    elif spotRound % 100 == 0 and spot > spotRound:
        return [spotRound, spotRound + 50, spotRound + 100]
    elif spotRound % 50 == 0:
        return [spotRound - 50, spotRound, spotRound + 50]

def findBNRange(spot):
    return [roundDown(spot, 100) - 100, roundDown(spot, 100), roundDown(spot, 100) + 100, roundDown(spot, 100) + 200]

def roundIt(x, base=5):
    return int(base * round(float(x)/base))

def roundDown(x, base=1):
    return int(math.floor(float(x) / base)) * base


def get_option_chain(symbol, spot_range, expiry='-'):
    # change symbol to upper case
    symbol = symbol.upper()

    # symbols and indices have different urls; load appropriate url
    if symbol == 'NIFTY' or symbol == 'NIFTY_BANK':
        Base_url =("https://www.nseindia.com/live_market/dynaContent/live_watch/option_chain/optionKeys.jsp?segmentLink=17&instrument=OPTIDX&symbol={}&date={}".format(symbol,expiry))
        print (Base_url)

    try:
        headers = {'Accept': '*/*',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
                   'X-Requested-With': 'XMLHttpRequest'
                   }
        page = requests.get(Base_url, headers=headers)
    except Exception as e:
        print ("Exception {} in getting data  for symbol {}".format(e, symbol))

    #print(page.status_code)
    #print(page.content)

    soup = BeautifulSoup(page.content, 'html.parser')
    # print (soup.prettify())

    table_it = soup.find_all("div", {"class": "opttbldata"})
    table_cls_1 = soup.find_all(id='octable')
    #     current_spot = soup.select_one('b')
    #     current_spot = re.findall("\d+\.\d+", current_spot.text.strip())

    #     print ('current_spot')
    #     # Using regular expressions to get only price
    #     # strip() is used to remove starting and trailing
    #     current_spot = float(current_spot[0])
    #     print (current_spot)
    #     # check price again if price in multiple of 100
    #     if current_spot % 100 == 0:
    #         time.sleep(30)
    #         get_option_chain('NIFTY')
    #     else:
    #         # get range
    #         spot_range = findNiftyRange(current_spot)
    
        
    # print (table_it)
    # print (table_cls_1)

    col_list = []

    for mytable in table_cls_1:
        table_head = mytable.find('thead')

        try:
            row = table_head.find_all('tr')
            for tr in row:
                cols = tr.find_all('th')
                for th in cols:
                    er = th.text
                    ee = er.encode('utf-8')
                    col_list.append(ee)
        except Exception as e:
            print ("No thead", e)

    col_list_fnl = [e for e in col_list if e not in ('CALLS', 'PUTS', 'Chart', '\xc2\xa0')]

    # print (col_list_fnl)


    table_cls_2 = soup.find(id='octable')
    all_trs = table_cls_2.find_all('tr')
    req_rows = table_cls_2.find_all('tr')

    new_table = pd.DataFrame(index=range(0, len(req_rows)-3), columns=col_list_fnl)

    row_marker = 0

    for row_number, tr_nos in enumerate(req_rows):
        if row_number < 1 or row_number == len(req_rows)-1:
            continue
        td_columns = tr_nos.find_all('td')

        select_cols = td_columns[1:22]
        cols_h = range(0, len(select_cols))

        for nu, column in enumerate(select_cols):
            utf_string = column.get_text().strip('\n\r\t": ')
            tr = utf_string.replace(',', '')
            new_table.ix[row_number, [nu]] = tr

        row_marker += 1

    # print ('#########################################################')
    # print (type(new_table))
    # find data for spot_range strikes
    # idx = new_table.index[new_table['Strike Price'].astype(float) == spot_range[0]].tolist()
    idx = new_table.index[new_table['Strike Price'].astype(float).isin(spot_range)].tolist()
    # print (idx)
    # print (new_table['Chng in OI'].iloc[idx])
    change_in_oi = new_table['Chng in OI'].iloc[idx]
    change_in_oi_ce = change_in_oi.iloc[:, 0]
    change_in_oi_pe = change_in_oi.iloc[:, 1]
    print ('change_in_oi_ce')
    print (change_in_oi_ce)
    print ('change_in_oi_pe')
    print (change_in_oi_pe)
    total_change_in_oi_pe = 0
    total_change_in_oi_ce = 0
    try:
        total_change_in_oi_ce = sum(map(int, change_in_oi_ce))  
        total_change_in_oi_pe = sum(map(int, change_in_oi_pe))
    except Exception as e:
        print ("Exception {} in OI from table".format(e))
    

    # print ('#########################################################')
    
    # print (list(new_table))
    # new_table.to_csv('Option_Chain_Table_{}.csv'.format(symbol))
    
    return [total_change_in_oi_ce, total_change_in_oi_pe]

# scrape option chain data ends


# Driver code

# compare 2 dates
from datetime import datetime
import calendar
import re
from dateutil import relativedelta

def getExpiryMonths(monthyear, retry):
    # define all expiry
    upcomingExpiry = config.UPCOMING_EXPIRY
    # find current monthyear
    currentMonthYear = (calendar.month_abbr[datetime.now().month]+str(datetime.now().year)).upper()
    # find next month date for expiry
    nextmonth = datetime.today() + relativedelta.relativedelta(months=1)
    nextMonthYear = (calendar.month_abbr[nextmonth.month]+str(nextmonth.year)).upper()

    # check if expiry is to be found of current month or next month
    # find MONYEAR in upcomingExpiry array using regex
    if monthyear == 0:
        regex = re.compile(".*({}).*".format(currentMonthYear))
    else:
        regex = re.compile(".*({}).*".format(nextMonthYear))

    thisMonthExpiry = [m.group(0) for l in upcomingExpiry for m in [regex.search(l)] if m]
    # filter validate upcoming expiry
    thisMonthExpiry = list(filter(greaterThanOrEqualToToday, thisMonthExpiry))

    # check if Expiry array is empty
    if len(thisMonthExpiry) < 1:
        if retry>0:
            retry -= 1
            return getExpiryMonths(nextMonthYear, retry)
    else:
        return thisMonthExpiry

# filter function for expiry which have passed
def greaterThanOrEqualToToday(x):
    # string to date
    expiryDate = datetime.strptime(x, "%d%b%Y").date()
    # today's date
    todayDate = datetime.now().date()
    if expiryDate < todayDate:
        return False
    return True
    

# compare 2 dates ends
thisMonthExpiry = getExpiryMonths(0, 1)

# thisMonthExpiry = ['2MAY2019', '9MAY2019', '16MAY2019', '23MAY2019', '30MAY2019']

def driverCode(symbol):

    # message text to be broadcasted on telegram channel
    message = ''

    with open('./prices.json') as json_data:
        prices = json.load(json_data)
        print(prices[symbol])
        current_spot = prices[symbol]

    if symbol == 'nifty':
        spot_range = findNiftyRange(current_spot)
        TELEGRAM_CHAT_ID = config.NIFTY_CHANNEL_ID
    elif symbol == 'nifty_bank':
        spot_range = findBNRange(current_spot)
        TELEGRAM_CHAT_ID = config.BN_CHANNEL_ID

    message += ("{} Open: {}\n".format(symbol, current_spot))
    message += ("{} Range: {}\n".format(symbol, spot_range))

    # if length=1; it's the last week of the month; only one expiry data is needed
    if len(thisMonthExpiry) == 1:
        total_weekly_oi_data = [0, 0]
        total_monthly_oi_data = get_option_chain(symbol, spot_range, thisMonthExpiry[0])
        message += ("Expiry: {}\n"
                    "Call Writing(Down): {}\n"
                    "Put Writing(Up): {}\n".format(thisMonthExpiry[0], total_monthly_oi_data[0], total_monthly_oi_data[1]))

        if total_monthly_oi_data[0] > 0 and total_monthly_oi_data[1] > 0:
            message += "Difference: {}\n".format(abs(total_monthly_oi_data[0] - total_monthly_oi_data[1]))

    # else if length>1; fetch first and last expiry data; and sum it
    elif len(thisMonthExpiry) > 1:
        total_monthly_oi_data = get_option_chain(symbol, spot_range, thisMonthExpiry[len(thisMonthExpiry) - 1])
        message += ("Expiry: {}\n"
                    "Call Writing(Down): {}\n"
                    "Put Writing(Up): {}\n".format(thisMonthExpiry[len(thisMonthExpiry) - 1], total_monthly_oi_data[0], total_monthly_oi_data[1]))
        
        if total_monthly_oi_data[0] > 0 and total_monthly_oi_data[1] > 0:
            message += "Difference: {}\n".format(abs(total_monthly_oi_data[0] - total_monthly_oi_data[1]))
        total_weekly_oi_data = get_option_chain(symbol, spot_range, thisMonthExpiry[0])
        message += ("\nExpiry: {}\n"
                    "Call Writing(Down): {}\n"
                    "Put Writing(Up): {}\n".format(thisMonthExpiry[0], total_weekly_oi_data[0], total_weekly_oi_data[1]))

        if total_weekly_oi_data[0] > 0 and total_weekly_oi_data[1] > 0:
            message += "Difference: {}\n".format(abs(total_weekly_oi_data[0] - total_weekly_oi_data[1]))
        
    print (message)
    send_telegram(message, TELEGRAM_CHAT_ID)

# driverCode('nifty_bank')
driverCode('nifty')


# from multiprocessing import Pool
# pool = Pool()
# args = ['nifty', 'banknifty']
# results = pool.map(driverCode, args)
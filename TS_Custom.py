import yfinance as yf
import talib as ta
from talib.abstract import *
import pandas as pd
import requests
import json
import pprint
import time
import datetime
from datetime import date
from datetime import datetime
from datetime import timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import re
import requests  # Module not included in standard python libarary
import urllib
import webbrowser
import TS_Client
import winsound
from tda import auth, client
from tda.orders.equities import equity_buy_limit
from tda.orders.equities import equity_buy_market
from tda.orders.common import Duration, EquityInstruction
from tda.orders.common import OrderStrategyType, OrderType, Session
from tda.orders.generic import OrderBuilder
import td_ameritrade_api
import config
import csv
from operator import itemgetter
import numpy as np
import traceback
# Importing the StringIO module
import io
from io import StringIO 
import time 
import os
import pandas_ta as pta
import concurrent.futures
import math
import glob

from polygon import *

pd.options.mode.chained_assignment = None

import concurrent.futures
from threading import Thread
import pytz

import mysql.connector

import warnings

import sqlalchemy

warnings.filterwarnings('ignore')


# Constants for API and localhost server
REFRESH_TOKEN = config.REFRESH_TOKEN
USER_ID = config.USER_ID
LOGIN_URL = config.LOGIN_URL
API_BASE_URL = config.API_BASE_URL


#Alpi's TS Client
ts_client = TS_Client.Client(REFRESH_TOKEN, USER_ID)



# variables
td_consumer_key = config.td_consumer_key
account_id = config.account_id
token_path = config.token_path
api_key = config.api_key
redirect_uri = config.redirect_uri

alphavintageapikey = config.alphavintageapikey


dt=datetime.now().strftime("%m-%d-%Y")

option_start_date = date.today() + timedelta(days=7)
option_end_date = date.today() + timedelta(days=30)

price_history_start_dt = date.today() - timedelta(days=5)
price_history_start_dt.strftime("%Y-%m-%d")
price_start_date = price_history_start_dt.strftime("%Y-%m-%d")


# name of csv file 
tradelog_filename = "C:/Test_AlphaOne/zlogs_alphaedge_tradelog" + ".csv"
positionlog_filename = "C:/Test_AlphaOne/zlogs_alphaedge_positionlog" + ".csv"
balancelog_filename = "C:/Test_AlphaOne/zlogs_alphaedge_balancelog" + ".csv"
orderlog_filename = "C:/Test_AlphaOne/zlogs_alphaedge_orderlog" + ".csv"
dfresults_filename = "zlogs_alphaedge_dfresults" + ".csv"

acct = ''
accounts = ts_client.getAccounts()
for account in accounts:
    if (account.number == config.ts_account_number):
        acct = account
        break


        
        
def get_Trade_Signals_fromDB():
    
    try:
        
        db,conn=getDBConnection()
        sql = "select BUY_STRATEGY,SELL_STRATEGY,BUY_LONG,BUY_SHORT,BUY_CALL,BUY_PUT from trade_settings"
        conn.execute(sql)
                
        results = conn.fetchall()
                
        for row in results: 
            buystrategy = row[0]
            sellstrategy = row[1]
            buylong = row[2]
            buyshort = row[3]
            buycall = row[4]
            buyput = row[5]
       
        db.commit()
        db.close()
        return buystrategy,sellstrategy,buylong,buyshort,buycall,buyput

    except Exception as e:
        traceback.print_exc()
        print(e)
    
        
def get_Sell_Strategy_Trade_Signals_fromDB():
    
    try:        
        db,conn=getDBConnection()
        sql = "select SELL_STRATEGY from trade_settings"
        conn.execute(sql)
                
        results = conn.fetchall()
                
        for row in results:             
            sellstrategy = row[0]            
       
        db.commit()
        db.close()
        
        return sellstrategy

    except Exception as e:
        traceback.print_exc()
        print(e)    
    
    
def printPriceHeaderRowtoCSV(pricelog_filename):
    # Print header row to csv file     
    # field names 
    pricelog_header_row = ['DATE-TIME', 'SYMBOL', 'CLOSE', 'PreviousBarValue', 'CurrentBarValue', 'STD_DEV', 'EMA9', 'EMA21']

    # writing to csv file pricelog
    with open(pricelog_filename, 'a', newline = '') as pricecsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(pricecsvfile) 

        # writing the fields 
        csvwriter.writerow(pricelog_header_row)

        #Close the file object
        pricecsvfile.close()


def printTradeHeaderRowtoCSV(tradelog_filename):
    # Print header row to csv file     
    # field names 
    tradelog_header_row = ['DATE-TIME', 'SYMBOL', 'TRADE', 'LIMIT_PRICE', 'QUANTITY', 'ORDERID']

    # writing to csv file tradelog
    with open(tradelog_filename, 'a', newline = '') as tradecsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(tradecsvfile) 

        # writing the fields 
        csvwriter.writerow(tradelog_header_row)

        #Close the file object
        tradecsvfile.close()
        
def printPositionHeaderRowtoCSV(positionlog_filename):
    # Print header row to csv file     
    # field names 
    positionlog_header_row = ['SYMBOL', 'POSITION_TYPE', 'QUANTITY', 'AVGPRICE','TOTALCOST','OPENPL', 'OPENPL_PERCENT', 'BID', 'ASK', 'MKTVALUE']

    # writing to csv file tradelog
    with open(positionlog_filename, 'a', newline = '') as positioncsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(positioncsvfile) 

        # writing the fields 
        csvwriter.writerow(positionlog_header_row)

        #Close the file object
        positioncsvfile.close()
        
def printOrderHeaderRowtoCSV(orderlog_filename):
    # Print header row to csv file     
    # field names 
    orderlog_header_row = ['DATE-TIME', 'SYMBOL', 'TRADE', 'LIMIT_PRICE', 'QUANTITY']

    # writing to csv file tradelog
    with open(orderlog_filename, 'a', newline = '') as ordercsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(ordercsvfile) 

        # writing the fields 
        csvwriter.writerow(orderlog_header_row)

        #Close the file object
        ordercsvfile.close()

def printPriceOutputtoCSV(pricelog_filename, symbol, close, PreviousBarValue, CurrentBarValue, stddeviation, ema9, ema21):

    #print to csv file
    with open(pricelog_filename, 'a', newline = '') as pricecsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(pricecsvfile) 
        # data row of csv file 
        row = [datetime.now().strftime('%H:%M'), symbol, close , PreviousBarValue, CurrentBarValue, stddeviation, ema9, ema21] 

        # writing the data rows 
        csvwriter.writerow(row)

        #Close the file object
        pricecsvfile.close()


def printTradeOutputtoCSV(tradelog_filename, date_time, optionSymbol, trade, limitPrice, quantity, orderid):

    #print trade to csv file
    with open(tradelog_filename, 'a', newline = '') as tradecsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(tradecsvfile) 
        # data row of csv file 
        row = [ date_time, str(optionSymbol), trade, limitPrice, quantity, orderid] 

        # writing the data rows 
        csvwriter.writerow(row)

        #Close the file object
        tradecsvfile.close()

#functions for alpha edge simulated account
def addOrderOutputtoCSV(orderlog_filename, date_time, optionSymbol, trade, limitprice, quantity):
    print("Add Order for", optionSymbol)
    #print orders to csv file
    with open(orderlog_filename, 'a', newline = '') as orderscsvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(orderscsvfile) 
        # data row of csv file 
        row = [ date_time, str(optionSymbol), trade, limitprice, quantity] 

        # writing the data rows 
        csvwriter.writerow(row)

        #Close the file object
        orderscsvfile.close()


def addPositionOutputtoCSV(positionlog_filename, positionsymbol, positiontype, quantity, limitprice) :
    
    try: 
        while True:
            if (is_locked(positionlog_filename)):
                time.sleep(0.5)
            else:    
                print("Add Position for ", positionsymbol)
                #add position to csv file
                with open(positionlog_filename, 'a', newline = '') as positioncsvfile: 
                    
                    # creating a csv writer object 
                    csvwriter = csv.writer(positioncsvfile) 
                    # data row of csv file 
                    row = [str(positionsymbol), positiontype, quantity, limitprice] 

                    # writing the data rows 
                    csvwriter.writerow(row)

                    #Close the file object
                    positioncsvfile.close()
                break
    except Exception as e:
        print(e)
      
        
def addprofitlossOutputtoCSV(balancelog_filename, profitloss):
    try:
        df_balances = pd.read_csv(balancelog_filename)
        print('ProfitLoss',profitloss)
        if (isinstance(profitloss, int)) or (isinstance(profitloss, float)):            
            df_balances['BALANCES'] = round((df_balances['BALANCES'] + profitloss),2)
            df_balances.to_csv(balancelog_filename, mode = 'w+', index=False)
        
    except Exception as e:
        print(e)
        
    
    
def updatePositionValuesinCSV(positionlog_filename):
    try:
        
        with open(positionlog_filename, 'r', newline = '') as positioncsvfile: 
            
            df_positions = pd.read_csv(positionlog_filename)

            for ind in df_positions.index:    
                symbol = df_positions['SYMBOL'][ind]
                positionType = df_positions['POSITION_TYPE'][ind]
                quantity =df_positions['QUANTITY'][ind]
                avgPrice = df_positions['AVGPRICE'][ind]
                totalCost = df_positions['TOTALCOST'][ind]
                bidPrice = df_positions['BID'][ind]
                askPrice = df_positions['ASK'][ind]
                openpl = df_positions['OPENPL'][ind]
                openpl_percent = df_positions['OPENPL_PERCENT'][ind]
                mktvalue = df_positions['MKTVALUE'][ind]
                
                quote_data=getQuote_TS(symbol)
                bidPrice = quote_data['Bid']
                askPrice = quote_data['Ask']
                
                val = symbol.split(" ")

                if (len(val) > 1):
                    df_positions['TOTALCOST'][ind] = round((avgPrice*100*quantity),2)
                    df_positions['MKTVALUE'][ind]=round((quantity * 100 * bidPrice),2)
                    df_positions['OPENPL'][ind]=round((quantity * 100 * (bidPrice-avgPrice)), 2)
                else :
                    if (positionType == 'Long') :                        
                        df_positions['TOTALCOST'][ind] = round((avgPrice*quantity),2)
                        df_positions['MKTVALUE'][ind]=round((quantity * bidPrice),2)
                        df_positions['OPENPL'][ind]=round((quantity * (bidPrice-avgPrice)), 2)
                        
                    if (positionType == 'Short') :
                        df_positions['TOTALCOST'][ind] = round((avgPrice*quantity),2)
                        df_positions['MKTVALUE'][ind]=round((quantity * bidPrice),2)
                        df_positions['OPENPL'][ind]=round((quantity * (avgPrice-askPrice)), 2)
                
                
                df_positions['OPENPL_PERCENT'][ind]=round(((df_positions['OPENPL'][ind]/df_positions['TOTALCOST'][ind])*100), 2)
                df_positions['BID'][ind]= bidPrice
                df_positions['ASK'][ind]= askPrice
  
                #lock(positionlog_filename)
                while True:
                    if (is_locked(positionlog_filename)):
                        time.sleep(0.5)
                    else:    
                        #print('newposition', df_positions)
                        df_positions.to_csv(positionlog_filename, mode = 'w+', index=False)
                        break
                #unlock(positionlog_filename)
                
                #Close the file object
                positioncsvfile.close()
    except Exception as e:
        traceback.print_exc()
        print(e)
        
        
        
def deletePositionfromCSV(positionlog_filename, symbol):
    try:
        
        while True:
            if (is_locked(positionlog_filename)):
                time.sleep(.5)
            else:    
                df_positions = pd.read_csv(positionlog_filename)
                df_positions = df_positions.drop(df_positions[(df_positions.SYMBOL == symbol)].index)
                df_positions.to_csv(positionlog_filename, mode = 'w+', index=False)
                break
    except Exception as e:
        print(e)
         
        
        
def getSymbolCallPositions_AEAccount(symbol):
    
    df_positions = pd.read_csv(positionlog_filename)
        
    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]

        val1 = val.split(" ")
        if (len(val1) > 1):
            if ((val1[0] == symbol) and val1[1].__contains__("C")):
                print('CallPositionFound', val)
                return True
    return False
    


def getSymbolPutPositions_AEAccount(symbol):
    df_positions = pd.read_csv(positionlog_filename)
    
    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]

        val1 = val.split(" ")
        if (len(val1) > 1):
            if ((val1[0] == symbol) and val1[1].__contains__("P")):
                print('PutPositionFound', val)
                return True
    return False


def getSymbolStockPositions_AEAccount(symbol):
    df_positions = pd.read_csv(positionlog_filename)

    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]
        if (val == symbol):
            return True
    return False

######################################################

def getBidAskPercent(content, callput):
    
    bid = list(list(content[callput].values())[0].values())[0][0]["bid"]
    ask = list(list(content[callput].values())[0].values())[0][0]["ask"]

    percent = round(((ask - bid) / bid)*100, 2)
        
    return percent

def getEquity_BidAskPercent(symbol,bid,ask):
    
    percent = round(((abs(ask - bid)) / bid)*100, 2)
        
    return percent


def getAskPrice(content, callput):
    bid = list(list(content[callput].values())[0].values())[0][0]["bid"]
    ask = list(list(content[callput].values())[0].values())[0][0]["ask"]

    mid = round((ask + bid) / 2, 2)
    
    return ask

def getMidPrice(content, callput):
    bid = list(list(content[callput].values())[0].values())[0][0]["bid"]
    ask = list(list(content[callput].values())[0].values())[0][0]["ask"]

    mid = round((ask + bid) / 2, 2)
    
    return mid

def getBidPrice(content, callput):
    bid = list(list(content[callput].values())[0].values())[0][0]["bid"]
    ask = list(list(content[callput].values())[0].values())[0][0]["ask"]

    mid = round((ask + bid) / 2, 2)
    return bid

def getProfitPrice(price,symbol):
    
    if (symbol == 'AMZN') or (symbol == 'MSFT') or (symbol == 'NVDA') or (symbol == 'NFLX') or (symbol == 'TSLA') :
        profit_price = "{:.2f}".format((round((price*1.02) / 0.05)) * 0.05)
    else :
        #profit_price = round((price*1.02), 2)
        profit_price = "{:.2f}".format((round((price*1.02) / 0.05)) * 0.05)
    
    return profit_price

def getProfitPriceFourPercent(price):
    
    profit_price = "{:.2f}".format((round((price*1.04) / 0.05)) * 0.05)
    
    return profit_price

def getStopPrice(price,symbol):
    
    if (symbol == 'AMZN') or (symbol == 'MSFT') or (symbol == 'NVDA') or (symbol == 'NFLX') or (symbol == 'TSLA') :
        stop_price = "{:.2f}".format((round((price*0.92) / 0.05)) * 0.05)
    else :
        #stop_price = round((price*0.92), 2)
        stop_price = "{:.2f}".format((round((price*0.92) / 0.05)) * 0.05)
        
    
    return stop_price


def getOrderID(content, callput):

    orderID = list(list(content[callput].values())[0].values())[0][0]["OrderID"]
   
    return orderID


def getSymbolCallPositions(acct,symbol):
    positions=ts_client.getPositions(acct)
    for position in positions:
        for key, val in position.items():
            if (key.lower() == "symbol"):
                val1 = val.split(" ")
                if ((val1[0] == symbol) and val1[1].__contains__("C")):
                    return True
    return False

def getSymbolPutPositions(acct,symbol):
    positions=ts_client.getPositions(acct)
    for position in positions:
        for key, val in position.items():
            if (key.lower() == "symbol"):
                val1 = val.split(" ")
                if ((val1[0] == symbol) and val1[1].__contains__("P")):
                    return True
    return False


def getSymbolStockPositions(acct,symbol):
    positions=ts_client.getPositions(acct)
    for position in positions:
        for key, val in position.items():
            if (key.lower() == "symbol"):
                if (val == symbol):
                    return True
    return False

def getSymbolStockOrders_Received(acct,symbol):
    orders = ts_client.getOrders(acct) 
    for order in orders:
        orderID = order['OrderID']
        val = order['Symbol']
 
        if (val == symbol and order['StatusDescription']=='Received' and order['Type']=='Buy'):
            return True
    return False


def getSymbolCallOrders_Received(acct,symbol):
    orders = ts_client.getOrders(acct) 
    for order in orders:
        orderID = order['OrderID']
        val = order['Symbol']
        val1 = order['Symbol'].split(" ")
        if (val1[0] == symbol and val1[1].__contains__("C") and order['StatusDescription']=='Received' and order['Type']=='Buy to Open'):
            return True
    return False
                    

def getSymbolPutOrders_Received(acct,symbol):
    orders = ts_client.getOrders(acct)
    for order in orders:
        orderID = order['OrderID']
        val = order['Symbol']
        val1 = order['Symbol'].split(" ")
        if (val1[0] == symbol and val1[1].__contains__("P") and order['StatusDescription']=='Received' and order['Type']=='Buy to Open'):
            return True
    return False



def getSymbolSellCallOrders_Received(acct,symbol):
    orders = ts_client.getOrders(acct) 
    for order in orders:
        orderID = order['OrderID']
        val = order['Symbol']
        val1 = order['Symbol'].split(" ")
        if (val1[0] == symbol and val1[1].__contains__("C") and order['StatusDescription']=='Received' and order['Type']=='Sell to Close'):
            return True
    return False
                    

def getSymbolSellPutOrders_Received(acct,symbol):
    orders = ts_client.getOrders(acct)
    for order in orders:
        orderID = order['OrderID']
        val = order['Symbol']
        val1 = order['Symbol'].split(" ")
        if (val1[0] == symbol and val1[1].__contains__("P") and order['StatusDescription']=='Received' and order['Type']=='Sell to Close'):
            return True
    return False


def getQuote_TS(symbol):
    try:
        print(symbol)
        data = ts_client.getOptionData(symbol)[0]
        #print("Last = ", data["Last"])
        #print("Bid = ", data["Bid"])
        #print("Ask = ", data["Ask"])
        
        return data
    
    except Exception as e:
        print(e)
        pass
        
    

def convetTDSymbolToTSSymbol(symbol):
    underlying, option_data = symbol.split('_')
    putcall = ''
    if option_data.__contains__('C'):
        putcall = 'C'
    elif option_data.__contains__('P'):
        putcall = 'P'
    dt = option_data[:option_data.index(putcall)]
    strike = option_data[option_data.index(putcall) + 1:]
    dtime = datetime.strptime(dt, '%m%d%y').strftime('%y%m%d')
    return underlying + ' ' + dtime + putcall + strike



# get option chain with max open interest and return only one strikeprice symbol
def getOptionChainDatafromTD_MaxOpenInterest(symbol):
    # get options data from td ameritrade
    base_url = 'https://api.tdameritrade.com/v1/marketdata/chains?&symbol={stock_ticker}'
    endpoint = base_url.format(stock_ticker=symbol)
    page=None
    try:
        page = requests.get(url=endpoint, params={'apikey': td_consumer_key, 'fromDate': option_start_date,
                                                  'toDate': option_end_date, 'strikeCount': 4, 'range': 'SAK'}, timeout=5)
    except: 
        try:
            page = requests.get(url=endpoint, params={'apikey': td_consumer_key, 'fromDate': option_start_date,
                                                  'toDate': option_end_date, 'strikeCount': 4, 'range': 'SAK'}, timeout=5)
        except Exception as e:
            print("Connection Exception - ", traceback.print_exec())
            pass
        
    pp = pprint.PrettyPrinter(indent=4)
    content = json.loads(page.content)
    #pp.pprint(content)
    print(content)
    return content


def getCallOptionSymbol_MaxOpenInterest(content):
    # retrieve call symbol
    call_optionSymbol = ''
    for key, value in content.items():
        if (key == "callExpDateMap"):
            for key1, value1 in value.items():
                expDate = key1
                maxopenInterest=0
                call_openInterestSymbol=""
                for key2, value2 in value1.items():
                    strikePrice = key2
                    print('strikePrice', strikePrice)
                    for val in value2:
                        for key3, value3 in val.items():
                            call_openInterest = val["openInterest"]
                            print ('call_openInterest',call_openInterest)
                            if call_openInterest > maxopenInterest:
                                maxopenInterest=call_openInterest
                                print ('maxopenInterest',maxopenInterest)
                                call_optionSymbol = convetTDSymbolToTSSymbol(val["symbol"])
                                call_openInterestSymbol=call_optionSymbol
                                print('call_openInterestSymbol', call_openInterestSymbol)
                            break
                        break
                    break
                break
            break
    return call_openInterestSymbol

def getPutOptionSymbol_MaxOpenInterest(content):
    # retrieve put symbol
    put_optionSymbol = ''
    for key, value in content.items():
        if (key == "putExpDateMap"):
            for key1, value1 in value.items():
                expDate = key1
                # print(key,value1)
                for key2, value2 in value1.items():
                    strikePrice = key2
                    for val in value2:
                        for key3, value3 in val.items():
                            put_optionSymbol = convetTDSymbolToTSSymbol(val["symbol"])
                            break
                        break
                    break
                break
            break
    return put_optionSymbol



# get option chain near the money and return only one strikeprice symbol
def getOptionChainDatafromTD(symbol):
    # get options data from td ameritrade
    base_url = 'https://api.tdameritrade.com/v1/marketdata/chains?&symbol={stock_ticker}'
    endpoint = base_url.format(stock_ticker=symbol)
    page=None
    try:
        page = requests.get(url=endpoint, params={'apikey': td_consumer_key, 'fromDate': option_start_date,
                                                  'toDate': option_end_date, 'strikeCount': 1, 'range': 'NTM'}, timeout=5)
    except: 
        try:
            page = requests.get(url=endpoint, params={'apikey': td_consumer_key, 'fromDate': option_start_date,
                                                  'toDate': option_end_date, 'strikeCount': 1, 'range': 'NTM'}, timeout=5)
        except Exception as e:
            print("Connection Exception - ", traceback.print_exec())
            pass
        
    pp = pprint.PrettyPrinter(indent=4)
    content = json.loads(page.content)
    #pp.pprint(content)
    #print(content)
    return content



def getCallOptionSymbol(content):
    # retrieve call symbol
    call_optionSymbol = ''
    for key, value in content.items():
        if (key == "callExpDateMap"):
            for key1, value1 in value.items():
                expDate = key1
                for key2, value2 in value1.items():
                    strikePrice = key2
                    for val in value2:
                        for key3, value3 in val.items():
                            call_optionSymbol = convetTDSymbolToTSSymbol(val["symbol"])
                            break
                        break
                    break
                break
            break
    return call_optionSymbol

def getPutOptionSymbol(content):
    # retrieve put symbol
    put_optionSymbol = ''
    for key, value in content.items():
        if (key == "putExpDateMap"):
            for key1, value1 in value.items():
                expDate = key1
                # print(key,value1)
                for key2, value2 in value1.items():
                    strikePrice = key2
                    for val in value2:
                        for key3, value3 in val.items():
                            put_optionSymbol = convetTDSymbolToTSSymbol(val["symbol"])
                            break
                        break
                    break
                break
            break
    return put_optionSymbol


def getCallOptionSymbol_TD(content):
    # retrieve call symbol
    call_optionSymbol = ''
    for key, value in content.items():
        if (key == "callExpDateMap"):
            for key1, value1 in value.items():
                expDate = key1
                for key2, value2 in value1.items():
                    strikePrice = key2
                    for val in value2:
                        for key3, value3 in val.items():
                            call_optionSymbol = val["symbol"]
                            break
                        break
                    break
                break
            break
    return call_optionSymbol

def getPutOptionSymbol_TD(content):
    # retrieve put symbol
    put_optionSymbol = ''
    for key, value in content.items():
        if (key == "putExpDateMap"):
            for key1, value1 in value.items():
                expDate = key1
                # print(key,value1)
                for key2, value2 in value1.items():
                    strikePrice = key2
                    for val in value2:
                        for key3, value3 in val.items():
                            put_optionSymbol = val["symbol"]
                            break
                        break
                    break
                break
            break
    return put_optionSymbol


    

def barssince7am(minutes_bars):
    if (int(datetime.now().strftime("%H"))>=19):
        current_time = datetime.strptime(datetime.now().strftime('%m-%d-%Y 19:00:00'),'%m-%d-%Y %H:%M:%S')
    else :
        current_time = datetime.strptime(datetime.now().strftime('%m-%d-%Y %H:%M:%S'),'%m-%d-%Y %H:%M:%S')
    #8 am ET
    start_time = datetime.strptime(date.today().strftime('%m-%d-%Y 07:00:00'),'%m-%d-%Y %H:%M:%S')
    
    #print(current_time)
    #print(start_time)    
       
    time_delta = current_time - start_time
    total_seconds = time_delta.total_seconds()
    minutes = total_seconds/60

    num_bars=minutes/int(minutes_bars)
    
    return num_bars


def getAccessToken() :
    
    ts_client.refreshAccessToken()
    access_token=ts_client.access_token
    
    return access_token



def getPriceHistory_TS(symbol,barsBack,minutes):
    # get price history from tradestation
    df = []    
    try:         
        ts_client.refreshAccessToken()
        access_token=ts_client.access_token
        
        dt = date.today().strftime('%m-%d-%Y')
        plusoneday = (date.today() + timedelta(days=1))
        future_time=plusoneday.strftime('%m-%d-%Yt%H:%M:%S')
        
        start_date = date.today().strftime('%Y-%m-%dT07:00:00Z')
        #added this for pre and post market data - 2-25-2022
        sessiontemplate = 'USEQPreAndPost'
        #sessiontemplate = 'USEQPre'
        #sessiontemplate = 'Default'
        
        #print('barsBack', barsBack)
        #print('future_time', future_time)
                
        endpoint = "https://sim-api.tradestation.com/v2/stream/barchart/" + symbol + "/" + minutes + "/Minute/" + str(barsBack) +"/" + future_time  + "?access_token=" + access_token + "&sessiontemplate=" + sessiontemplate 
      
        
        
        response = requests.get(url=endpoint)
        # response_json = response.decode('utf-8')
        
        data = str(response.content).split('\\r\\n')
        data_list=[]
        for dict_ in data:
            dict_ = dict_.replace("b'", "").replace("END'", "")
            if dict_!='' and type(dict_) == str:
                data_line=json.loads(dict_)
                data_list.append(data_line)
        df=pd.DataFrame(data_list)
        
        #convert time from epoch to current timezone and replace in the timestamp in df
        df['TimeStamp'] = df['TimeStamp'].str.replace(r"\D+",'', regex=True)
        df['TimeStamp']= pd.to_datetime(df['TimeStamp'],unit='ms')
        df['TimeStamp']= df['TimeStamp'] - timedelta(hours=5)
        
        # sort df by TimeStamp
        df = df.sort_values(by='TimeStamp', ascending=True)     
        
        df['TimeStamp'] = df['TimeStamp'].dt.strftime('%m-%d-%Y %I:%M:%S %p')
        
        
        #print('TIMESTAMP',df['TimeStamp'][len(df)-1])
        df_dt=datetime.strptime(df['TimeStamp'][len(df)-1], "%m-%d-%Y %I:%M:%S %p").strftime('%m-%d-%Y')
        curr_dt=datetime.now().strftime("%m-%d-%Y")
        
                                      
        if (df_dt!=curr_dt):
            print ('Last Date was ', df_dt)
            print("Date was MISMATCH. Had to reset BARSBACK for ", symbol)
            barsBack=int(barsBack)-2
            
            endpoint =   "https://api.tradestation.com/v2/data/symbols/search/N=MSFT" + "?access_token=" + access_token
            requests.get(url=endpoint)
            
            endpoint = "https://sim-api.tradestation.com/v2/stream/barchart/" + symbol + "/" + minutes + "/Minute/" + str(barsBack) +"/" + future_time  + "?access_token=" + access_token + "&sessiontemplate=" + sessiontemplate 
            response = requests.get(url=endpoint)
            
            data = str(response.content).split('\\r\\n')
            data_list=[]
            for dict_ in data:
                dict_ = dict_.replace("b'", "").replace("END'", "")
                if dict_!='' and type(dict_) == str:
                    data_line=json.loads(dict_)
                    data_list.append(data_line)
            df=pd.DataFrame(data_list)

            #convert time from epoch to current timezone and replace in the timestamp in df
            df['TimeStamp'] = df['TimeStamp'].str.replace(r"\D+",'', regex=True)
            df['TimeStamp']= pd.to_datetime(df['TimeStamp'],unit='ms')
            df['TimeStamp']= df['TimeStamp'] - timedelta(hours=5)
            df['TimeStamp'] = df['TimeStamp'].dt.strftime('%m-%d-%Y %I:%M:%S %p')
            
            
            df_dt=datetime.strptime(df['TimeStamp'][len(df)-1], "%m-%d-%Y %I:%M:%S %p").strftime('%m-%d-%Y')
            if (df_dt == curr_dt) :
                print("Dataframe is ACCURATE after resetting barsBack")
        
            if (df_dt!=curr_dt):
                print("Date was MISMATCH. DATAFRAME is EMPTY", symbol)
                #df = [] 
                return df
            
        
        df['SYMBOL']=symbol
        calculate_ema(df,9)
        calculate_ema(df,21)
        calculate_ema(df,50)
        calculate_ema(df,200)
        calculate_rsi(df,21)
        calculate_atr(df)
        calculate_bollingerbands(df,20,2)  
        calculate_stochastic_fast(df)
        calculate_stochastic_slow(df)        
        calculate_linear_regression_slope(df,14)
        calculate_ema9_angle(df)
        calculate_vwap(df)
        
            
        #calculate_pivotpoints(df,symbol)  
        #calculate_bollingerbandwith(df)         
        
        df['stddeviation'] = ta.STDDEV(df['Close'], 20)       
        df['avgupvolume'] = ta.SUM(df['UpVolume'], timeperiod=5)/5
        df['avgdownvolume'] = ta.SUM(df['DownVolume'], timeperiod=5)/5
        
        # remove  columns
        df = df.drop(['DownTicks', 'TotalTicks', 'Status', 'UnchangedTicks','UnchangedVolume','UpTicks','OpenInterest'], axis=1)  
        
        
        
    except Exception as e:
        print(e)
        #pass

    return df


def calculate_vwap(df):
    
    #df['vwap'] = (df['TotalVolume'] * (df['Close'] + df['High'] + df['Low']) / 3).cumsum() / df['TotalVolume'].cumsum()
    v = df['TotalVolume'].values
    tp = df['Close'].values
    df['vwap']=(tp * v).cumsum() / v.cumsum()
    
    return df



def calculate_bollingerbands(df, n, m):
    # takes dataframe on input
    # n = smoothing length
    # m = number of standard deviations away from MA
    
    #typical price
    TP = (df['High'] + df['Low'] + df['Close']) / 3
    # but we will use Adj close instead for now, depends
    
    data = TP
    #data = df['Adj Close']
    
    # takes one column from dataframe
    B_MA = pd.Series((data.rolling(n, min_periods=n).mean()))
    sigma = data.rolling(n, min_periods=n).std() 
    
    BU = pd.Series(B_MA + m * sigma)
    BL = pd.Series(B_MA - m * sigma)
    
    df['upper_bb'] = BU
    df['lower_bb'] = BL
    df['middle_bb'] = B_MA    
    
    return df

def calculate_bollingerbandwith(df):   
    current_bandwidth = ((df['upper_bb']  - df['lower_bb'])/df['middle_bb']) * 100    
    df['Bandwidth']=current_bandwidth
    
    return df


def calculate_ema(df, length):
    i=0
    smoothing=2/(length+1)
    
    ema = []
    
    
    for i in range(0,len(df)):
        if i==0:
            ema.append(df['Close'][0])
        else:    
            ema_l=(df['Close'][i] * (smoothing) + (ema[i-1] * (1 - (smoothing))))
            ema.append(ema_l)
    emalength = str('ema') + str(length)
    #print (emalength)
    df[emalength]=pd.Series(ema)
    return df


def calculate_ema_ha(df, length):
    i=0
    smoothing=2/(length+1)
    
    ema = []
    
    
    for i in range(0,len(df)):
        if i==0:
            ema.append(df['HA_Close'][0])
        else:    
            ema_l=(df['HA_Close'][i] * (smoothing) + (ema[i-1] * (1 - (smoothing))))
            ema.append(ema_l)
    emalength = str('ema') + str(length)
    #print (emalength)
    df[emalength]=pd.Series(ema)
    return df


def calculate_rsi(df,n):
           
    prices=df['Close'].tolist()
    
    m = 0 
    change = []
    #  Loop to calculate the change in prices
    while m < len(prices):
        if m == 0:
            change.append(0)
        else:
            change.append(prices[m]-prices[m-1]) # Calculate change
        m += 1
                
    #df['change'] = change           
    
    i=0
    upPrices=[]
    downPrices=[]
    #  Loop to hold up and down price movements
    while i < len(prices):
        if i == 0:
            upPrices.append(0)
            downPrices.append(0)
        else:
            if (prices[i]-prices[i-1])>0:
                upPrices.append(prices[i]-prices[i-1])
                downPrices.append(0)
            else:
                downPrices.append(prices[i]-prices[i-1])
                upPrices.append(0)
        i += 1
    
    #df['gain'] = upPrices
    #df['loss'] = downPrices    
    
    x = 0
    avg_gain = []
    avg_loss = []
    #  Loop to calculate the average gain and loss
    while x < len(upPrices):
        if x <n-1:
            avg_gain.append(0)
            avg_loss.append(0)
        else:
            sumGain = 0
            sumLoss = 0
            y = x-(n-1)
            while y<=x:
                sumGain += upPrices[y]
                sumLoss += downPrices[y]
                y += 1
            avg_gain.append(sumGain/n)
            avg_loss.append(abs(sumLoss/n))
        x += 1
    
    #df['avg_gain']=avg_gain
    #df['avg_loss']=avg_loss
    
    
    p = 0
    RS = []
    RSI = []
    #  Loop to calculate RSI and RS
    while p < len(prices):
        if p <n-1:
            RS.append(0)
            RSI.append(0)
        else:
            RSvalue = (avg_gain[p]/avg_loss[p])
            RS.append(RSvalue)
            RSI.append(100 - (100/(1+RSvalue)))
        p+=1
        
    #df['RS']=RS
    df['RSI']=RSI
           


def calculate_atr(df):   
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    atr = true_range.rolling(14).sum()/14
    df['ATR']=atr
    
    return df


def calculate_period_atr(df,period):  
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    atr = true_range.rolling(period).sum()/period
    df['ATR']=atr
    return df

def calculate_period_atr_ha(df,period):  
    high_low = df['HA_High'] - df['HA_Low']
    high_close = np.abs(df['HA_High'] - df['HA_Close'].shift())
    low_close = np.abs(df['HA_Low'] - df['HA_Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    atr = true_range.rolling(period).sum()/period
    df['HA_ATR']=atr
    return df


def calculate_pivotpoints(df,symbol):
        
    pivot_df = []    
    try: 
        ts_client.refreshAccessToken()
        access_token=ts_client.access_token
        dt = date.today().strftime('%m-%d-%Y')
        plusoneday = (date.today() + timedelta(days=1))
        future_time=plusoneday.strftime('%m-%d-%Yt%H:%M:%S')
        
        start_date = date.today().strftime('%Y-%m-%dT07:00:00Z')
        #added this for pre and post market data - 2-25-2022
        sessiontemplate = 'USEQPre'
        
        #print('barsBack', barsBack)
        #print('future_time', future_time)
                
        endpoint = "https://sim-api.tradestation.com/v3/marketdata/barcharts/" + symbol +  "?barsback=1&access_token=" + access_token + "&sessiontemplate=" + sessiontemplate 
        
        #print(endpoint)
        response = requests.get(url=endpoint)

        response_json = response.json()
               
        pivot_df = pd.DataFrame(response_json)
        

        prev_high=float(pivot_df['Bars'].iloc[0]['High'])
        prev_low=float(pivot_df['Bars'].iloc[0]['Low'])
        prev_close=float(pivot_df['Bars'].iloc[0]['Close'])
        
              
        df['pp'] = (prev_high + prev_low + prev_close)/3
        df['r1'] = (2 * df['pp']) - prev_low
        df['r2'] = df['pp'] + (prev_high - prev_low)
        df['s1'] = (2 * df['pp']) - prev_high
        df['s2'] = df['pp'] - (prev_high - prev_low)
        
                
    except Exception as e:
        print(e)
        traceback.print_exc()
        
    return df


def calculate_stochastic_fast(df):
    
    
    # Define periods
    k_period = 14
    d_period = 3
    # Adds a "n_high" column with max value of previous 14 periods
    n_high_df = df['High'].rolling(k_period).max()
    # Adds an "n_low" column with min value of previous 14 periods
    n_low_df = df['Low'].rolling(k_period).min()
    # Uses the min/max values to calculate the %k (as a percentage)
    df['K_fast'] = (df['Close'] - n_low_df) * 100 / (n_high_df - n_low_df)
    # Uses the %k to calculates a SMA over the past 3 values of %k
    df['D_fast'] = df['K_fast'].rolling(d_period).mean()

        
    return df

def calculate_stochastic_slow(df):
    
    
    # Define periods
    k_period = 21
    d_period = 10
    # Adds a "n_high" column with max value of previous 14 periods
    n_high_df = df['High'].rolling(k_period).max()
    # Adds an "n_low" column with min value of previous 14 periods
    n_low_df = df['Low'].rolling(k_period).min()
    # Uses the min/max values to calculate the %k (as a percentage)
    k_fast = (df['Close'] - n_low_df) * 100 / (n_high_df - n_low_df)
    # Uses the %k to calculates a SMA over the past 3 values of %k
    d_fast = k_fast.rolling(3).mean()

    # Slow Stochastic
    df['K_slow'] = d_fast
    df['D_slow'] = df['K_slow'].rolling(window = d_period).mean()
        
    return df

def calculate_ema9_angle(df):   
    
    ema9_difference = df['ema9'] - df['ema9'].shift()
    ema_lst=[]
    for x in range(0, len(ema9_difference)):
        ema_lst.append(math.atan(ema9_difference[x]/1)*(180/math.pi))
    df['ema9_angle'] =  pd.Series(ema_lst)
    
    return df

def calculate_linear_regression_slope(df,timeperiod):   
    
    linear_reg_slope = ta.LINEARREG_SLOPE(df['Close'], timeperiod)
    df['linear_reg_slope']=pd.Series(linear_reg_slope)
    return df

    


def getPriceHistory_AlphaVantage(symbol,minutes):
    # get price history from alpha vantage
    df = []
    try: 
        # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + symbol + '&interval='+ minutes + '&apikey=' + alphavintageapikey
        response = requests.get(url)
        df = response.json()
        
    except Exception as e:
        #print(e)
        pass

    return df


def ts_to_datetime(ts) -> str:
    return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%m-%d-%Y %H:%M:%S %p')


def getPriceHistory_YF(symbol):
    # get price history from yahoo finance
    power = yf.Ticker(symbol)
    df = power.history(interval="1m", start=price_start_date, prepost=True)
     
    #print (df)

    return df


def getQuote_YF(symbol):    
    
    quote=yf.Ticker(symbol)
    
    return quote.info



#get price history from Polygon
def getPriceHistory_Polygon(symbol, minutes):
    key = "KoUi9iHbj7n9hlYmZ2aHpiT5ljMDutz1"
    df = []
    try:
    # RESTClient can be used as a context manager to facilitate closing the underlying http session
    # https://requests.readthedocs.io/en/master/user/advanced/#session-objects
        with RESTClient(key) as client:
            #dt = date.today().strftime('%Y-%m-%d')
            #plusoneday = (date.today() + timedelta(days=1))
            #future_time=plusoneday.strftime('%m-%d-%Yt%H:%M:%S')
            to = datetime.now()
            from_ = to - timedelta(days=1)            
            resp = client.stocks_equities_aggregates(symbol, minutes, "minute", from_.strftime('%Y-%m-%d'),
                                                     to.strftime('%Y-%m-%d'), unadjusted=False)
           
            data_list = []
            print(type(resp))
            for result in resp.results:
                result = str(result).replace("'v'", "\"TotalVolume\"").replace("'o'", "\"Open\"") \
                    .replace("'c'", "\"Close\"").replace("'h'", "\"High\"")\
                    .replace("'l'", "\"Low\"").replace("'t'","\"TimeStamp\"") \
                    .replace("'n'", "\"TotalTicks\"").replace("'vw'", "\"vwap\"")
                # dt = ts_to_datetime(result["t"])
                data_line = json.loads(result)
                data_list.append(data_line)

            df = pd.DataFrame(data_list)
            
            df = df.drop(['TotalTicks'], axis=1)
             #convert time from epoch to current timezone and replace in the timestamp in df
            #df['TimeStamp'] = df['TimeStamp'].str.replace(r"\D+",'', regex=True)
            df['TimeStamp']= pd.to_datetime(df['TimeStamp'],unit='ms')
            df['TimeStamp']= df['TimeStamp'] - timedelta(hours=5)
            df['TimeStamp'] = df['TimeStamp'].dt.strftime('%m-%d-%Y %I:%M:%S %p')

                        

    except Exception as e:
        print(e)

    return df


def printToConsole(df):
    
    try:
                
        ema9 = df['ema9']
        ema21 = df['ema21']
        ema50 = df['ema50']
        rsi = df['RSI']
        
        stddeviation = ta.STDDEV(df['Close'], 21)
        vwap = df['vwap']
        upper = df['upper_bb'] 
        middle = df['middle_bb']
        lower = df['lower_bb']


        last_key = ema9.keys()[-1]
        previous_key = ema9.keys()[-2]
        previous_key2 = ema9.keys()[-3]

        if (df['Close'][previous_key]) > (df['Open'][previous_key]):
            CurrentBarValue = "Green"
        else :
            CurrentBarValue = "Red"
            
        if (df['Close'][previous_key2]) > (df['Open'][previous_key2]):
            PreviousBarValue = "Green"
        else :
            PreviousBarValue = "Red"


        #Print to Console
        print("TIME: ", datetime.now().strftime('%H:%M'))
        print("ClOSE_LK =", str(df['Close'][last_key]))   
        print('OPEN_LK =', df['Open'][last_key])
        print('EMA9_LK =', ema9[last_key])
        print('EMA21_LK =', ema21[last_key])
        print('EMA50_LK =', ema50[last_key])
        print('EMA200_LK =', ema200[last_key])
        print('VWAP_LK =', vwap[last_key])
        print('VWAP_LK =', vwap[last_key])
        print('STD DEV_LK =', stddeviation[last_key])
        print('UPPERBBAND_LK=', upper[last_key])
        print('LOWERBBAND_LK=', lower[last_key])
        print ('-----')     
        print("ClOSE_PK =", str(df['Close'][previous_key]))   
        print('OPEN_PK =', df['Open'][previous_key])
        print('EMA9_PK =', ema9[previous_key])
        print('EMA21_PK =', ema21[previous_key])
        print('EMA50_PK =', ema50[previous_key])
        print('EMA200_PK =', ema200[previous_key])
        print('VWAP_PK =', vwap[previous_key])
        print('STD DEV_PK =', stddeviation[previous_key])
        print('UPPERBBAND_PK=', upper[previous_key])
        print('LOWERBBAND_PK=', lower[previous_key])
        print ('-----')        
        print ('PreviousBarValue = ', PreviousBarValue)
        print ('CurrentBarValue = ', CurrentBarValue) 
        
    except Exception as e:
        #print(e)
        pass

    

def getHA(df):
    try:
               
        df['HA_Close']=(df['Open']+ df['High']+ df['Low']+df['Close'])/4
        
        idx = df.index.name
        df.reset_index(inplace=True)

        ha_close_values = df['HA_Close'].values

        length = len(df)
        ha_open = np.zeros(length, dtype=float)
        ha_open[0] = (df['Open'][0] + df['Close'][0]) / 2

        for i in range(0, length - 1):
            ha_open[i + 1] = (ha_open[i] + ha_close_values[i]) / 2

        df['HA_Open'] = ha_open

        df['HA_High']=df[['HA_Open','HA_Close','High']].max(axis=1)
        df['HA_Low']=df[['HA_Open','HA_Close','Low']].min(axis=1)
        
        #df = df.drop(['Open', 'High', 'Low', 'Close'], axis=1)  # remove old columns
        #df = df.rename(columns={"HA_Open": "Open", "HA_High": "High", "HA_Low": "Low", "HA_Close": "Close"})
        #df = df[['Close','Open', 'High', 'Low']]  # reorder columns
    
    except Exception as e:
        print(e)
        
    return df



def getQuote(symbol):
    # get quote from tradestation
    quote = []
    try: 
        ts_client.refreshAccessToken()
        access_token=ts_client.access_token
        
        endpoint = "https://sim-api.tradestation.com/v2/data/quote/" + symbol + "?access_token=" + access_token
        response = requests.get(url=endpoint)
        
        data = str(response.content).replace("b'", "").replace("]\'","").replace('[{','{')
       
        quote = json.loads(str(data))
        #data_list=json.loads(str(data))
        
        
    except Exception as e:
        print(e)
        #traceback.print_exc()
        
    return quote

def getClosePrice_TS(symbol, ts):
    # get price history from tradestation
    dfCurrentClose = []
    
    try: 

        ts_client.refreshAccessToken()
        access_token=ts_client.access_token

        #dt = date.today().strftime('%m-%d-%Y')

        price_time=ts.strftime('%m-%d-%Yt%H:%M:%S')

        endpoint = "https://sim-api.tradestation.com/v2/stream/barchart/" + symbol + "/5/Minute/1/" + price_time + "?access_token=" + access_token
        #print("endPoint=",endpoint)
        response = requests.get(url=endpoint)
        # response_json = response.decode('utf-8')
        data = str(response.content).split('\\r\\n')
        data_list=[]
        for dict_ in data:
            dict_ = dict_.replace("b'", "").replace("END'", "")
            if dict_!='' and type(dict_) == str:
                data_line=json.loads(dict_)
                data_list.append(data_line)
        dfCurrentClose = pd.DataFrame(data_list)
        #print(dfCurrentClose)
        
    except Exception as e:
        print(e)

    return dfCurrentClose


def crossover_nbars(symbol,df, fastma, slowma,n):
    
    key_list=[]
    calculate_ema(df,fastma)
    calculate_ema(df,slowma)
    df['fastma'] = df['ema'+ str(fastma)] 
    #ta.EMA(df['Close'], fastma)
    df['slowma'] = df['ema'+ str(slowma)] 
    #ta.EMA(df['Close'], slowma)
    
    previous_fast = df['fastma'].shift(1)
    previous_slow = df['slowma'].shift(1)
    
    for i in range(n):
        key = df['fastma'].keys()[-1*(i+1)]
        key_list.append(key)

            
    for key in key_list:
        crossover = ((df['fastma'][key] > df['slowma'][key]) and (previous_fast[key] < previous_slow[key]))  
        
        if (crossover):
            #print ('CROSSOVER of', symbol, fastma, slowma)
            
            #print('CROSSOVER AT KEY', key)
            #print(df['fastma'])
            #print(df['slowma'])
            #print ('Fast', df['fastma'][key])
            #print ('Slow', df['slowma'][key])
            #print ('Previous Fast', previous_fast[key])
            #print ('Previous Slow', previous_slow[key])
            
            return crossover
    
    return crossover

def crossunder_nbars(symbol, df, fastma, slowma,n):
    
    key_list=[]
    calculate_ema(df,fastma)
    df['fastma'] = df['ema'+ str(fastma)]
    calculate_ema(df,slowma)
    df['slowma'] = df['ema'+ str(slowma)] 

    previous_fast = df['fastma'].shift(1)
    previous_slow = df['slowma'].shift(1)
    
    for i in range(n):     
        key = df['fastma'].keys()[-1*(i+1)]        
        key_list.append(key)
            
    for key in key_list:
        crossunder = ((df['fastma'][key] < df['slowma'][key]) and (previous_fast[key] > previous_slow[key]))  
        

        if (crossunder):
            #print ('CROSSUNDER of', symbol, fastma, slowma)
            
            #print('CROSSOVER AT KEY', key)
            #print(df['fastma'])
            #print(df['slowma'])
            #print ('Fast', df['fastma'][key])
            #print ('Slow', df['slowma'][key])
            #print ('Previous Fast', previous_fast[key])
            #print ('Previous Slow', previous_slow[key])
            
            return crossunder
        
    return crossunder


def check_previous_green_bars(df,n):
    
    key_list=[]
    
    for i in range(n):
        key = df['HA_Close'].keys()[-1*(i+1)]
        key_list.append(key)

    for key in key_list:
        
        greenbar = df['HA_Close'][key] > df['HA_Open'][key]   
        if (greenbar==False):            
            return greenbar
    
    return greenbar

def check_previous_red_bars(df,n):
    
    key_list=[]
    
    for i in range(n):
        key = df['HA_Close'].keys()[-1*(i+1)]
        key_list.append(key)

    for key in key_list:
        
        redbar = df['HA_Close'][key] < df['HA_Open'][key]   
        if (redbar==False):            
            return redbar
    
    return redbar

def crossover_4bars(symbol, df, fastma, slowma):
    
    key_list=[]
    calculate_ema(df,fastma)
    df['fastma'] = df['ema'+ str(fastma)] 
    calculate_ema(df,slowma)
    df['slowma'] = df['ema'+ str(slowma)] 
    
    previous_fast = df['fastma'].shift(1)
    previous_slow = df['slowma'].shift(1)
    
    last_key = df['fastma'].keys()[-1]
    key_list.append(last_key)
    previous_key = df['fastma'].keys()[-2]
    key_list.append(previous_key)
    previous_key2 = df['fastma'].keys()[-3]
    key_list.append(previous_key2)
    previous_key3 = df['fastma'].keys()[-4]
    key_list.append(previous_key3)
    previous_key4 = df['fastma'].keys()[-5]
    key_list.append(previous_key4)
    
    for key in key_list:
        crossover = ((df['fastma'][key] >= df['slowma'][key]) and (previous_fast[key] <= previous_slow[key]))  

        if (crossover):
            #print ('CROSSOVER of', symbol, fastma, slowma)
            return crossover
    
    return crossover

def crossunder_4bars(symbol, df, fastma, slowma):
    
    key_list=[]
    calculate_ema(df,fastma)
    df['fastma'] = df['ema'+ str(fastma)]
    #ta.EMA(df['Close'], fastma)
    calculate_ema(df,slowma)
    df['slowma'] = df['ema'+ str(slowma)]
    #ta.EMA(df['Close'], slowma)
    
    previous_fast = df['fastma'].shift(1)
    previous_slow = df['slowma'].shift(1)
    
    last_key = df['fastma'].keys()[-1]
    key_list.append(last_key)
    previous_key = df['fastma'].keys()[-2]
    key_list.append(previous_key)
    previous_key2 = df['fastma'].keys()[-3]
    key_list.append(previous_key2)
    previous_key3 = df['fastma'].keys()[-4]
    key_list.append(previous_key3)
    previous_key4 = df['fastma'].keys()[-5]
    key_list.append(previous_key4)
    
    for key in key_list:
        crossunder = ((df['fastma'][key] <= df['slowma'][key]) and (previous_fast[key] >= previous_slow[key]))  

        if (crossunder):
            #print ('CROSSUNDER of', symbol, fastma, slowma)
            return crossunder
    
    return crossunder

def crossover(df,fastma,slowma, last_key,previous_key):
    
    calculate_ema(df,fastma)
    df['fastma'] = df['ema'+ str(fastma)] 
    #ta.EMA(df['Close'], fastma)

    calculate_ema(df,slowma)
    df['slowma'] = df['ema'+ str(slowma)] 
    #ta.EMA(df['Close'], slowma)
   
    previous_fast = df['fastma'].shift(1)
    previous_slow = df['slowma'].shift(1)
           
    crossover = ((df['fastma'][last_key] >= df['slowma'][last_key]) and (previous_fast[last_key] <= previous_slow[last_key]))  
    
    if (crossover):
        print ('CROSSOVER of', fastma, slowma)
    
    return crossover

def crossunder(df,fastma,slowma, last_key, previous_key):
    
    calculate_ema(df,fastma)
    df['fastma'] = df['ema'+ str(fastma)]
     
    #ta.EMA(df['Close'], fastma)
    calculate_ema(df,fastma)
    df['slowma'] = df['ema'+ str(fastma)] 
    #ta.EMA(df['Close'], slowma)

    previous_fast = df['fastma'].shift(1)
    previous_slow = df['slowma'].shift(1)
    
    crossunder = ((df['fastma'][last_key] <= df['slowma'][last_key]) and (previous_fast[last_key] >= previous_slow[last_key]))
    
    if (crossunder):
        print ('CROSSUNDER of', fastma, slowma)
        
    return crossunder

def pricecrossoversma9(df, last_key, previous_key):
    
    df['sma9'] = ta.SMA(df['Close'][:9])

    pricecrossoversma9 = ((df['fastma'][last_key] <= df['slowma'][last_key]) and (previous_fast[last_key] >= previous_slow[last_key]))
    
    print ('pricecrossoversma9 = ', pricecrossoversma9)

    return pricecrossoversma9




def ichimoku(df):
    
    calculate_ema(df,3)
    ema3 =  df['ema3']
    #ta.EMA(df['Close'], 3)
    calculate_ema(df,5)
    ema5 =  df['ema5'] 
    #ta.EMA(df['Close'], 5)
    calculate_ema(df,5)
    ema9 = df['ema9']
    #ta.EMA(df['Close'], 9)
    calculate_ema(df,21)
    ema21 = df['ema21']
    #ta.EMA(df['Close'], 21)

    stddeviation = ta.STDDEV(df['Close'], 21)

    last_key = ema9.keys()[-1]
    previous_key = ema9.keys()[-2]
    previous_key2 = ema9.keys()[-3]
    previous_key3 = ema9.keys()[-4]

    # Define length of Tenkan Sen or Conversion Line
    cl_period = 9 

    # Define length of Kijun Sen or Base Line
    bl_period = 26  

    # Define length of Senkou Sen B or Leading Span B
    lead_span_b_period = 52  

    # Define length of Chikou Span or Lagging Span
    lag_span_period = 26  

    # Calculate conversion line
    high_9 = df['High'].rolling(cl_period).max()
    low_9 = df['Low'].rolling(cl_period).min()
    df['conversion_line'] = (high_9 + low_9) / 2

    # Calculate based line
    high_26 = df['High'].rolling(bl_period).max()
    low_26 = df['Low'].rolling(bl_period).min()
    df['base_line'] = (high_26 + low_26) / 2

    # Calculate leading span A
    df['lead_span_A'] = ((df.conversion_line + df.base_line) / 2).shift(lag_span_period)

    #Calculate current cloud A
    df['currentcloud_A'] = (df.conversion_line + df.base_line) / 2

    # Calculate leading span B
    high_52 = df['High'].rolling(lead_span_b_period).max()
    low_52 = df['Low'].rolling(lead_span_b_period).min()
    df['lead_span_B'] = ((high_52 + low_52) / 2).shift(lag_span_period)

    #Calculate current cloud B
    df['currentcloud_B'] = (high_52 + low_52) / 2

    # Calculate lagging span - chikou
    df['lagging_span'] = df['Close'].shift(-lag_span_period)

    #print(df)

    return df['conversion_line'][previous_key], df['base_line'][previous_key], df['lead_span_A'][previous_key], df['lead_span_B'][previous_key], df['lagging_span'][previous_key], df['currentcloud_A'][previous_key], df['currentcloud_B'][previous_key]

def buyEQUITY(symbol, quantity, limitprice, tradelog_filename):
    print("SENDING TEST BUY STOCK ORDER for ", symbol)
         
    response = ts_client.submitOrder(acct, "EQ", "DYP", "LIMIT", quantity, symbol, "BUY", limit_price=limitprice)       

    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY STOCK DONE")
        
        #print tradeout to csv file
        #printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'BUY', limitprice, quantity, response[0]['OrderID'])
        
def sellEQUITY(symbol, quantity, limitprice, tradelog_filename):
    print("SENDING TEST SELL TO CLOSE STOCK ORDER for ", symbol)
    
    response = ts_client.submitOrder(acct, "EQ", "DYP", "LIMIT", quantity, symbol, "SELL", limit_price=limitprice)    
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("SELL STOCK DONE")
        
        #print tradeout to csv file
        #printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'SELL', limitprice, quantity, response[0]['OrderID'])
        

def sellshortEQUITY(symbol, quantity, limitprice, tradelog_filename):
    print("SENDING TEST SELLSHORT STOCK ORDER for ", symbol)
    
    
    response = ts_client.submitOrder(acct, "EQ", "DYP", "LIMIT", quantity, symbol, "SELLSHORT", limit_price=limitprice)    
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("SELLSHORT STOCK DONE")

        
        #print tradeout to csv file
        #printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'SELLSHORT', limitprice, quantity, response[0]['OrderID'])

        
def buytocoverEQUITY(symbol, quantity, limitprice, tradelog_filename):
    print("SENDING TEST BUY TO COVER STOCK ORDER for ", symbol)
    
    
    response = ts_client.submitOrder(acct, "EQ", "DYP", "LIMIT", quantity, symbol, "BUYTOCOVER", limit_price=limitprice)    
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("SELLSHORT STOCK DONE")

        
        #print tradeout to csv file
        #printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'BUYTOCOVER', limitprice, quantity, response[0]['OrderID'])


def buyEQUITYStopLossProfitTarget(symbol, quantity, df, last_key, previous_key, ema5_pk, ema9_pk, tradelog_filename):
    print("SENDING TEST BUY STOCK ORDER for ", symbol)
    ##winsound.Beep(4000, 500)
    quote = getQuote(symbol)
    print ('symbol' , symbol)
    midPrice = round((quote['Bid'] + quote['Ask'])/2, 2)
    profit_price = round(midPrice * 1.0010, 2)
    stop_price = round(midPrice * 0.99, 2)
    limitprice = midPrice
    response = ts_client.submitOrder_BUY_EQUITY_StopLossProfitTarget(acct, "EQ", "DYP", "LIMIT", quantity, symbol, "BUY",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY STOCK DONE")


        #print tradeout to csv file
        printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'BUY', limitprice, quantity, response[0]['OrderID'])
        
        
def sellshortEQUITYStopLossProfitTarget(symbol, quantity, df, last_key, previous_key, ema5_pk, ema9_pk, tradelog_filename):
    print("SENDING TEST SELLSHORT STOCK ORDER for ", symbol)
    ##winsound.Beep(4000, 500)
    quote = getQuote(symbol)
    print ('symbol' , symbol)
    midPrice = round((quote['Bid'] + quote['Ask'])/2, 2)
    profit_price = round(midPrice * 0.9990, 2)
    stop_price = round(midPrice * 1.01, 2)
    limitprice = midPrice
    response = ts_client.submitOrder_SELLSHORT_EQUITY_StopLossProfitTarget(acct, "EQ", "DYP", "LIMIT", quantity, symbol, "SELLSHORT",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("SELLSHORT STOCK DONE")

               
         #print tradeout to csv file
        printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'SELLSHORT', limitprice, quantity, response[0]['OrderID'])


def sellCALLatAtrTrailingStop(highs, lows, closes,buyEntryClosePrice, buyEntryTime, numBarsMinutes=25,atrMultiplier=2,atrPeriod=14):

    highs = np.array(highs)
    closes = np.array(closes)
    lows = np.array(lows)

    atr = ta.ATR(highs, lows, closes, timeperiod=atrPeriod)

    last_key=len(atr)-1
    prev_key = len(atr) - 2

    targetPrice = buyEntryClosePrice + (atr[last_key] * atrMultiplier)
    timeSinceEntry =  datetime.utcnow() - buyEntryTime
    minsSinceEntry=int(timeSinceEntry.total_seconds()/60)
    print ('###########')
    print("Current Close Price = ",closes[last_key])
    print("Target Sell Price = ",targetPrice)
    print("Previous Bar's Low Price = ",lows[prev_key])
    print("Buy Close Price = ",buyEntryClosePrice)
    print("Buy Time = ",buyEntryTime)
    print ('Current Time = ', datetime.utcnow())
    print("Minutes Since Entry = ",minsSinceEntry)
    print ('###########')


    if (minsSinceEntry < numBarsMinutes):
        #Sell if target price is reached
        if (closes[last_key] >= targetPrice):
            return True

    elif (minsSinceEntry >= numBarsMinutes):
        #this bars close price
        if (closes[last_key] < lows[prev_key]) or (closes[last_key] >= targetPrice):
            return True

    return False


def sellPUTatAtrTrailingStop(highs, lows, closes,buyEntryClosePrice, buyEntryTime, numBarsMinutes=25,atrMultiplier=2,atrPeriod=14):

    highs = np.array(highs)
    closes = np.array(closes)
    lows = np.array(lows)

    atr = ta.ATR(highs, lows, closes, timeperiod=atrPeriod)

    last_key=len(atr)-1
    prev_key = len(atr) - 2

    targetPrice = buyEntryClosePrice - (atr[last_key] * atrMultiplier)
    timeSinceEntry =  datetime.utcnow() - buyEntryTime
    minsSinceEntry=int(timeSinceEntry.total_seconds()/60)
    print ('###########')
    print("Current Close Price = ",closes[last_key])
    print("Target Sell Price = ",targetPrice)
    print("Previous Bar's High Price = ",highs[prev_key])
    print("Buy Close Price = ",buyEntryClosePrice)
    print("Buy Time = ",buyEntryTime)
    print ('Current Time = ', datetime.utcnow())
    print("Minutes Since Entry = ",minsSinceEntry)
    print ('###########')


    if (minsSinceEntry < numBarsMinutes):
        #Sell if target price is reached
        if (closes[last_key] <= targetPrice):
            return True

    elif (minsSinceEntry >= numBarsMinutes):
        #this bars close price
        if (closes[last_key] > highs[prev_key]) or (closes[last_key] <= targetPrice):
            return True

    return False

def buyCALL(symbol, quantity, limitprice, call_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY CALL ORDER for ", call_optionSymbol)
    ##winsound.Beep(4000, 500)
    #midPrice = getMidPrice(content, "callExpDateMap")
    #askPrice = getAskPrice(content, "callExpDateMap")
    #limitprice = round(askPrice+0.01,2)
    
    response = ts_client.submitOrder(acct, "OP", "DAY", "LIMIT", quantity, call_optionSymbol, "BUYTOOPEN",
                                     limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY CALL DONE")
        #getOptionChainData(call_optionSymbol)



def sellCALL(symbol, quantity, limitprice, val, tradelog_filename, content):    
    print("SENDING TEST SELL CALL ORDER for ", val)
    ##winsound.Beep(4000, 500)
    #midPrice = getMidPrice(content, "callExpDateMap")
    #bidPrice = getBidPrice(content, "callExpDateMap")
    #limitprice = round(bidPrice-0.01,2)
    
    response = ts_client.submitOrder(acct, "OP", "DAY", "LIMIT", quantity, val, "SELLTOCLOSE", limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("SELL CALL DONE")
        
        
        
def buyPUT(symbol, quantity, limitprice, put_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY PUT ORDER for ", put_optionSymbol)
    ##winsound.Beep(4000, 500)
    #midPrice = getMidPrice(content, "putExpDateMap")
    #askPrice = getAskPrice(content, "putExpDateMap")
    #limitprice = round(askPrice+0.01,2)
    
    response = ts_client.submitOrder(acct, "OP", "DAY", "LIMIT", quantity, put_optionSymbol, "BUYTOOPEN",
                                     limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY PUT DONE")
        #getOptionChainData(put_optionSymbol)



def sellPUT(symbol, quantity, limitprice, val, tradelog_filename, content):
    print("SENDING TEST SELL PUT ORDER for ", val)
    ##winsound.Beep(4000, 500)
    #midPrice = getMidPrice(content, "putExpDateMap")
    #bidPrice = getBidPrice(content, "putExpDateMap")
    #limitprice = round(bidPrice-0.01,2)
    
    response = ts_client.submitOrder(acct, "OP", "DAY", "LIMIT", quantity, val, "SELLTOCLOSE",
                                     limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("SELL PUT DONE")

        

def buyCALLStopLossProfitTarget(symbol, quantity, df, call_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY CALL ORDER for ", call_optionSymbol)
    ##winsound.Beep(4000, 500)
    midPrice = getMidPrice(content, "callExpDateMap")
    print('midPrice', midPrice)
    profit_price = getProfitPrice(midPrice,symbol)
    stop_price = getStopPrice(midPrice,symbol)
    limitprice = midPrice
    
    response = ts_client.submitOrderStopLossProfitTarget(acct, "OP", "DAY", "LIMIT", quantity, call_optionSymbol, "BUYTOOPEN",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY CALL DONE")

                          
        
        
def buyPUTStopLossProfitTarget(symbol, quantity, df, put_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY PUT ORDER for ", put_optionSymbol)
    ##winsound.Beep(4000, 500)
    midPrice = getMidPrice(content, "putExpDateMap")
    profit_price = getProfitPrice(midPrice,symbol)
    stop_price = getStopPrice(midPrice,symbol)
    limitprice = midPrice

    response = ts_client.submitOrderStopLossProfitTarget(acct, "OP", "DAY", "LIMIT", quantity, put_optionSymbol, "BUYTOOPEN",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY PUT DONE")
                
       

        
def buyCALLwithTrailingPercent(symbol, quantity, df, call_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY CALL ORDER for ", call_optionSymbol)
    ##winsound.Beep(4000, 500)
    midPrice = getMidPrice(content, "callExpDateMap")
    profit_price = getProfitPrice(midPrice,symbol)
    stop_price = getStopPrice(midPrice,symbol)
    limitprice = midPrice
    
    response = ts_client.submitOrderwithTrailingPercent(acct, "OP", "DAY", "LIMIT", quantity, call_optionSymbol, "BUYTOOPEN",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY CALL DONE")
                

        
def buyPUTwithTrailingPercent(symbol, quantity, df, put_optionSymbol, tradelog_filename, content):
    print("SENDING TEST PUT CALL ORDER for ", put_optionSymbol)
    ##winsound.Beep(4000, 500)
    midPrice = getMidPrice(content, "putExpDateMap")
    profit_price = getProfitPrice(midPrice,symbol)
    stop_price = getStopPrice(midPrice,symbol)
    limitprice = midPrice
    
    response = ts_client.submitOrderwithTrailingPercent(acct, "OP", "DAY", "LIMIT", quantity, put_optionSymbol, "BUYTOOPEN",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY PUT DONE")
                       
        

        
def updateOrderCALL(orderID, bidPrice, symbol, quantity, df, val, tradelog_filename, content):
    print("SENDING TEST UPDATE CALL ORDER for ", val)
    ##winsound.Beep(4000, 500)

    print("Updating: ", orderID)
    
    limitprice = bidPrice
    
    response = ts_client.updateOrder("LIMIT", quantity, val, orderID, limit_price=limitprice)
    print (response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("UPDATE SELL CALL DONE")

        #print tradeout to csv file
        #printTradeOutputtoCSV(tradelog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'UPDATED_SELL_CALL', limitprice, quantity, response[0]['OrderID'])

def updateOrderPUT(orderID, bidPrice, symbol, quantity, df, val, tradelog_filename, content):
    print("SENDING TEST UPDATE PUT ORDER for ", val)
    ##winsound.Beep(4000, 500)
   
    print("Updating: ", orderID)
    
    limitprice = bidPrice
    
    response = ts_client.updateOrder("LIMIT", quantity, val, orderID, limit_price=limitprice)
    print (response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("UPDATE SELL PUT DONE")
 
        
        

def updateOrder_TrailProfitPercent(orderID, bidPrice, symbol, quantity, df, val, tradelog_filename, content):
    print("SENDING TEST UPDATE ORDER for ", val)
    #winsound.Beep(4000, 500)
    newBidPrice = "{:.2f}".format((round((bidPrice*0.98) / 0.05)) * 0.05)
    limitprice = newBidPrice
    
    print("Updating: ", orderID)
    
    response = ts_client.updateOrder("LIMIT", quantity, val, orderID, limit_price=limitprice)
    print (response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("UPDATE SELL WITH STOP LOSS % DONE")

        

        
def buyCALLwithProfitTarget(symbol, quantity, df, call_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY CALL ORDER for ", call_optionSymbol)
    #winsound.Beep(4000, 500)
    midPrice = getMidPrice(content, "callExpDateMap")
    profit_price = getProfitPriceFourPercent(midPrice)
    stop_price = getStopPrice(midPrice)
    limitprice = midPrice
    
    response = ts_client.submitOrderwithProfitTarget(acct, "OP", "DAY", "LIMIT", quantity, call_optionSymbol, "BUYTOOPEN",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY CALL DONE")
               

        
def buyPUTwithProfitTarget(symbol, quantity, df, put_optionSymbol, tradelog_filename, content):
    print("SENDING TEST BUY PUT ORDER for ", put_optionSymbol)
    #winsound.Beep(4000, 500)
    midPrice = getMidPrice(content, "putExpDateMap")
    profit_price = getProfitPriceFourPercent(midPrice)
    stop_price = getStopPrice(midPrice)
    limitprice = midPrice

    response = ts_client.submitOrderwithProfitTarget(acct, "OP", "DAY", "LIMIT", quantity, put_optionSymbol, "BUYTOOPEN",stop_price, profit_price, limit_price=limitprice)
    print("Response=", response)
    if (response[0]["OrderStatus"] == "Ok"):
        print("BUY PUT DONE")
        #getOptionChainData(put_optionSymbol)


# sell all positions from AE simulated account
def closeAllPositions_Amount(positions,amount,position_type):
    for position in positions:        
        try:            
            openpl = float(position['OPENPL'])
            positionType = position['POSITION_TYPE']
            
            if (((openpl >= amount) and (position_type=="ALL")) or ((openpl >= amount) and (positionType==position_type))) :

                symbol = position['SYMBOL']                
                quantity =position['QUANTITY']
                avgPrice = position['AVGPRICE']
                bidPrice = position['BID']
                askPrice = position['ASK']            
                openpl_percent = position['OPENPL_PERCENT']
                #limitprice = round((askPrice + bidPrice)/2,2)
                limitprice = round(float(bidPrice),2)


                val = symbol.split(" ")
                
                '''
                # get price history from tradestation
                barsBack = 50
                minutes = 5
                if (len(val) > 1):
                    df = getPriceHistory_TS(val[0],str(barsBack), str(minutes))
                else :
                    df = getPriceHistory_TS(symbol,str(barsBack), str(minutes))


                #convert current candles to heiken-ashi
                df = getHA(df)
                

                ema9 = df['ema9']


                #retrieve keys
                last_key = ema9.keys()[-1]
                previous_key = ema9.keys()[-2]
                previous_key2 = ema9.keys()[-3]
                previous_key3 = ema9.keys()[-4]
                
                '''
                
                
                datetime_now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
                strategy = 'MANUAL_CLOSE_AMOUNT'

                # if the position is an option then sell according to the rules below
                if (len(val) > 1):
                    if (val[1].__contains__("C")) : 
                        print (datetime.now().strftime('%H:%M:%S'))
                        print('Sell Call for ', symbol, 'at', limitprice)

                        profitloss = round((quantity * 100 * (limitprice-avgPrice)),2)
                        
                        # Sell Call in TS
                        #sellCALL(symbol, quantity, limitprice, val, tradelog_filename, content)
                
                        #add to DB
                        addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'SELL_CALL', limitprice, quantity)
                        addprofitlossOutputtoDB(profitloss)
                        deletePositionfromDB(symbol)
                        
                        #add log to DB
                        #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELL_CALL', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                        addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELL_CALL', limitprice, quantity, profitloss, bidPrice, askPrice)



                    if (val[1].__contains__("P")):
                        print (datetime.now().strftime('%H:%M:%S'))
                        print('Sell Put for ', symbol, 'at', limitprice)

                        profitloss = round((quantity * 100 * (limitprice-avgPrice)),2)
                        
                        #Sell Put in TS
                        #sellPUT(symbol, quantity, limitprice, val, tradelog_filename, content) 

                        #add to DB
                        addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'SELL_PUT', limitprice, quantity)
                        addprofitlossOutputtoDB(profitloss)
                        deletePositionfromDB(symbol)
                        
                        #add log to DB
                        #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELL_PUT', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                        addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELL_PUT', limitprice, quantity, profitloss, bidPrice, askPrice)

                # if the position is a stock then sell according to the rules below
                else :
                    if (positionType == 'Long'):
                        print (datetime.now().strftime('%H:%M:%S'))
                        print('Sell Stock for ', symbol, 'at', limitprice)

                        profitloss = round((quantity * (limitprice-avgPrice)),2)
                        
                        #sellEQUITY in TS
                        sellEQUITY(symbol, quantity, limitprice, tradelog_filename)
                    
                        #add to DB
                        addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'SELL_STOCK', limitprice, quantity)
                        addprofitlossOutputtoDB(profitloss)
                        deletePositionfromDB(symbol)
                        
                        #add log to DB
                        #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELL_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                        addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELL_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice)


                    if (positionType == 'Short'):        
                        print (datetime.now().strftime('%H:%M:%S'))
                        limitprice = round(float(askPrice),2)
                        print('Buy to Cover for ', symbol, 'at', limitprice)

                        profitloss = round((quantity * (avgPrice-limitprice)),2)
                        
                        #buytocoverEQUITY in TS
                        buytocoverEQUITY(symbol, quantity, limitprice, tradelog_filename)
                    
                        #add to DB
                        addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'BUYTOCOVER_STOCK', limitprice, quantity)
                        addprofitlossOutputtoDB(profitloss)
                        deletePositionfromDB(symbol)
                        
                        #add log to DB
                        #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'BUYTOCOVER_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                        addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'BUYTOCOVER_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice)


                #print("******************")


        except Exception as e:
            traceback.print_exc()
            print(e)
            pass

        
# closePosition from AE simulated account
def closePosition(symbol,positionType,quantity,avgPrice,totalCost,openpl,openpl_percent,bidPrice,askPrice,mktvalue):
    
    try:

                
        quantity = int(quantity)
        avgPrice = float(avgPrice)
        totalCost = float(totalCost)
        openpl = float(openpl)
        openpl_percent = float(openpl_percent)
        bidPrice = float(bidPrice)
        askPrice = float(askPrice)
        mktvalue = float(mktvalue)

        limitprice = round(float(bidPrice),2)
        #limitprice = round((askPrice + bidPrice)/2,2)
               
        val = symbol.split(" ")
        
        
        '''
        # get price history from tradestation
        barsBack = 210
        minutes = 5
        if (len(val) > 1):
            df = getPriceHistory_TS(val[0],str(barsBack), str(minutes))
        else :
            df = getPriceHistory_TS(symbol,str(barsBack), str(minutes))
                   
            
        #convert current candles to heiken-ashi
        df = getHA(df)               
             
        ema9 = df['ema9']
        
        #retrieve keys
        last_key = ema9.keys()[-1]
        previous_key = ema9.keys()[-2]
        previous_key2 = ema9.keys()[-3]
        previous_key3 = ema9.keys()[-4]
        '''
        
        datetime_now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        strategy = 'MANUAL_CLOSE'
        
        # if the position is an option then sell according to the rules below
        if (len(val) > 1):
            if (val[1].__contains__("C")):
                #if (openpl_percent > 2) or (openpl_percent < -8) :
                print (datetime.now().strftime('%H:%M:%S'))
                print('Sell Call for ', symbol, 'at', limitprice)
                profitloss = round((quantity * 100 * (limitprice-avgPrice)),2)
                
                # Sell Call in TS
                #sellCALL(symbol, quantity, limitprice, val, tradelog_filename, content)
                    
                #add to DB
                addOrderOutputtoDB(datetime_now, str(symbol), 'SELL_CALL', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)
                
                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELL_CALL', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELL_CALL', limitprice, quantity, profitloss, bidPrice, askPrice)


            if (val[1].__contains__("P")):
                #if (openpl_percent > 2) or (openpl_percent < -8) :
                print (datetime.now().strftime('%H:%M:%S'))
                print('Sell Put for ', symbol, 'at', limitprice)
                profitloss = round((quantity * 100 * (limitprice-avgPrice)),2)
                
                #Sell Put in TS
                #sellPUT(symbol, quantity, limitprice, val, tradelog_filename, content)    
                    
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H-%M-%S'), str(symbol), 'SELL_PUT', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)
                
                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELL_PUT', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELL_PUT', limitprice, quantity, profitloss, bidPrice, askPrice)
                                           
                                           
        # if the position is a stock then sell according to the rules below
        else :
            if (positionType == 'Long'):
                print (datetime.now().strftime('%H:%M:%S'))
                print('Sell Stock for ', symbol, 'at', limitprice)
                profitloss = round((quantity * (limitprice-avgPrice)),2)
                
                #sellEQUITY in TS
                sellEQUITY(symbol, quantity, limitprice, tradelog_filename)
                
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H-%M-%S'), str(symbol), 'SELL_STOCK', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)
                
                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELL_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELL_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice)
                    
            if (positionType == 'Short'):        
                print (datetime.now().strftime('%H:%M:%S'))
                limitprice = round(float(askPrice),2)
                
                print('Buy to Cover for ', symbol, 'at', limitprice)
                profitloss = round(float((quantity * (avgPrice-limitprice))),2)
                
                #buytocoverEQUITY in TS
                buytocoverEQUITY(symbol, quantity, limitprice, tradelog_filename)
                    
                    
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'BUYTOCOVER_STOCK', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)
                
                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'BUYTOCOVER_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'BUYTOCOVER_STOCK', limitprice, quantity, profitloss, bidPrice, askPrice)
                
  

        #print("******************")
        
        
    except Exception as e:
        traceback.print_exc()
        print(e)
        


# openPosition from AE simulated account
def openPosition(symbol,optiontype,quantity):
    
    try:

        # Strategy Code
        
        # get options data from td ameritrade
        content = getOptionChainDatafromTD(symbol)

        # retrieve call symbol
        call_optionSymbol = getCallOptionSymbol(content)

        # retrieve put symbol
        put_optionSymbol = getPutOptionSymbol(content)
        
        
        val = symbol.split(" ")
        
        
        '''
        # get price history from tradestation
        barsBack = 50
        minutes = 5
        if (len(val) > 1):
            df = getPriceHistory_TS(val[0],str(barsBack), str(minutes))
        else :
            df = getPriceHistory_TS(symbol,str(barsBack), str(minutes))
                   
            
        #convert current candles to heiken-ashi
        df = getHA(df)               
             
        
        ema9 = df['ema9']
        atr = df['ATR']
        
        
        #retrieve keys
        last_key = ema9.keys()[-1]
        previous_key = ema9.keys()[-2]
        previous_key2 = ema9.keys()[-3]
        previous_key3 = ema9.keys()[-4]
        '''
        
        
        datetime_now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        strategy = 'MANUAL_OPEN'
        
        if (optiontype == "BUYEQUITY"):
            
            # Buy Equity
            #if (not getSymbolStock_LONG_PositionsfromDB(symbol)) :

            # get quote from tradestation
            quote = getQuote(symbol)
            askPrice = round((quote['Ask']), 2)
            bidPrice = round((quote['Bid']), 2)
            midPrice = round((askPrice + bidPrice)/2,2)
            #openPrice = round((df['Open'][last_key]), 2)
            limitprice = float(round(askPrice+0.01,2))

            equity_bidaskpercent = getEquity_BidAskPercent(symbol,bidPrice,askPrice)
            print("equity_bidaskpercent:",equity_bidaskpercent, 'for', symbol) 


            if (equity_bidaskpercent <= 1) :
                if (quantity == "Default"):
                    quantity=int(600/(limitprice))
                else :
                    quantity=int(quantity)


                print("ALL CONDITIONS FOR BUY STOCK MET FOR ", symbol)

                totalCost = round((limitprice*quantity),2)
                openpl = round((quantity * (bidPrice-limitprice)), 2)
                openpl_percent = round(((openpl/totalCost)*100), 2)
                mktvalue = mktvalue = round((quantity * bidPrice),2)
                #triggerprice = round(quantity * atr[last_key],2)
                #triggerprice = round(quantity * 0.40 * atr[last_key],2)
                triggerprice = 0
                previous_openpl = round((quantity * (bidPrice-limitprice)), 2)
                #profit_target = limitprice * 1.0065
                profit_target = round(limitprice * 1.03,2)
                #profit_target = limitprice + float(triggerprice/quantity)
                stop_loss = round(limitprice * 0.995,2)

                #buy Equity in TS
                buyEQUITY(symbol, quantity, limitprice, tradelog_filename)

                winsound.Beep(4000, 500)

                # add to DB
                addOrderOutputtoDB(datetime_now, str(symbol), 'BUY_STOCK', limitprice, quantity)
                addPositionOutputtoDB(datetime_now, str(symbol), 'Long', quantity, limitprice,totalCost, openpl, openpl_percent, bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)


                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'BUY_STOCK', limitprice, quantity, '', bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'BUY_STOCK', limitprice, quantity, '', bidPrice, askPrice)


            
        if (optiontype == "SELLSHORTEQUITY"): 
            
            # SellShort Equity
            #if (not getSymbolStock_SHORT_PositionsfromDB(symbol)) :

            # get quote from tradestation
            quote = getQuote(symbol)
            #midPrice = round((quote['Bid'] + quote['Ask'])/2, 2)
            askPrice = float(round((quote['Ask']), 2))
            bidPrice = float(round((quote['Bid']), 2))
            midPrice = round((askPrice + bidPrice)/2,2)
            #openPrice = float(round((df['Open'][last_key]), 2))
            #limitprice = bidPrice
            limitprice = float(round(bidPrice-0.01,2))
            #limitprice = midPrice
            equity_bidaskpercent = getEquity_BidAskPercent(symbol,bidPrice,askPrice)
            print("equity_bidaskpercent:",equity_bidaskpercent, 'for', symbol)

            if (equity_bidaskpercent <= 1) :

                if (quantity == "Default"):
                    quantity=int(600/(limitprice))
                else :
                    quantity=int(quantity)

                print("ALL CONDITIONS FOR SELLSHORT MET FOR ", symbol)

                totalCost = round((limitprice*quantity),2)
                openpl = round((quantity * (limitprice-askPrice)), 2)
                openpl_percent = round(((openpl/totalCost)*100), 2)
                mktvalue = round((quantity * bidPrice),2)

                #triggerprice = round(quantity * atr[last_key],2)
                #triggerprice = round(quantity * 0.40 * atr[last_key],2)
                triggerprice = 0
                previous_openpl = round((quantity * (limitprice-askPrice)), 2)
                #profit_target = limitprice * 0.935
                profit_target = round(limitprice * 0.97,2)
                #profit_target = limitprice - float(triggerprice/quantity)
                stop_loss = round(limitprice * 1.005,2)

                #sellshort Equity in TS
                sellshortEQUITY(symbol, quantity, limitprice, tradelog_filename)

                winsound.Beep(4000, 500)

                #add to DB
                addOrderOutputtoDB(datetime_now, str(symbol), 'SELLSHORT_STOCK', limitprice, quantity)
                addPositionOutputtoDB(datetime_now, str(symbol), 'Short', quantity, limitprice, totalCost, openpl,openpl_percent,bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)


                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELLSHORT_STOCK', limitprice, quantity, '', bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, symbol, strategy,'SELLSHORT_STOCK', limitprice, quantity, '', bidPrice, askPrice)

            
       
        if (optiontype == "CALL"): 
            #if (not getSymbolCallPositionsfromDB(symbol)) :
                
            bidaskpercent = getBidAskPercent(content, "callExpDateMap")
            print("bidaskpercent:",bidaskpercent,'% for', symbol)

            if bidaskpercent <= 5 :
                askPrice = getAskPrice(content, "callExpDateMap")
                bidPrice = getBidPrice(content, "callExpDateMap")
                limitprice = askPrice
                print('limit',limitprice)

                if (quantity == "Default"):
                    quantity=int(600/(limitprice*100))
                else :
                    quantity=int(quantity)

                print("ALL CONDITIONS FOR BUY CALL OPTION MET FOR ", symbol)

                totalCost = round((limitprice*quantity*100),2)
                openpl = round((quantity * 100* (bidPrice-limitprice)), 2)
                openpl_percent = round(((openpl/totalCost)*100), 2)
                mktvalue = mktvalue = round((quantity * 100 * bidPrice),2)
                #triggerprice = round(quantity * 0.70 * atr[last_key]*100,2)
                triggerprice = 0
                previous_openpl = round((quantity * 100 * (bidPrice-limitprice)), 2)
                profit_target = round(limitprice * 1.0065,2)
                stop_loss = round(limitprice * 0.995,2)

                #Buy Call Options in TS                    
                buyCALL(symbol, quantity, limitprice, call_optionSymbol, tradelog_filename, content)


                winsound.Beep(4000, 500)

                # add to DB
                addOrderOutputtoDB(datetime_now, str(call_optionSymbol), 'BUY_CALL', limitprice, quantity)
                addPositionOutputtoDB(datetime_now, str(call_optionSymbol), 'Long', quantity, limitprice,totalCost, openpl, openpl_percent, bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)


                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, str(call_optionSymbol), strategy,'BUY_CALL', limitprice, quantity, '', bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, str(call_optionSymbol), strategy,'BUY_CALL', limitprice, quantity, '', bidPrice, askPrice)
                    


        if (optiontype == "PUT"):             
            #if (not getSymbolPutPositionsfromDB(symbol)) :
                
            bidaskpercent = getBidAskPercent(content, "putExpDateMap")
            print("bidaskpercent:",bidaskpercent,'% for', symbol)

            if bidaskpercent <= 5 :
                askPrice = getAskPrice(content, "putExpDateMap")
                bidPrice = getBidPrice(content, "putExpDateMap")
                limitprice = askPrice
                print('limit',limitprice)

                if (quantity == "Default"):
                    quantity=int(600/(limitprice*100))
                else :
                    quantity=int(quantity)

                print("ALL CONDITIONS FOR BUY PUT OPTION MET FOR ", symbol)

                totalCost = round((limitprice*quantity*100),2)
                openpl = round((quantity * 100* (bidPrice-limitprice)), 2)
                openpl_percent = round(((openpl/totalCost)*100), 2)
                mktvalue = mktvalue = round((quantity * 100 * bidPrice),2)
                #triggerprice = round(quantity * 0.70 * atr[last_key] * 100,2)
                triggerprice = 0
                previous_openpl = round((quantity * 100 * (bidPrice-limitprice)), 2)
                profit_target = round(limitprice * 1.0065,2)
                stop_loss = round(limitprice * 0.995,2)


                #Buy Put Options in TS                    
                buyPUT(symbol, quantity, limitprice, put_optionSymbol, tradelog_filename, content)

                winsound.Beep(4000, 500)

                # add to DB
                addOrderOutputtoDB(datetime_now, str(put_optionSymbol), 'BUY_PUT', limitprice, quantity)
                addPositionOutputtoDB(datetime_now, str(put_optionSymbol), 'Long', quantity, limitprice,totalCost, openpl, openpl_percent, bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)


                #add log to DB
                #addOrderLogOutputtoDB(datetime_now, str(put_optionSymbol), strategy,'BUY_PUT', limitprice, quantity, '', bidPrice, askPrice,df)
                addOrderLogOutputtoDB_noDF(datetime_now, str(put_optionSymbol), strategy,'BUY_PUT', limitprice, quantity, '', bidPrice, askPrice)
                    
        
    except Exception as e:
        traceback.print_exc()
        print(e)
        



# openPosition from AE simulated account
# not used anymore
def openPosition_manually(symbol,optiontype):
    
    try:

        # Strategy Code
        
        # get options data from td ameritrade
        content = getOptionChainDatafromTD(symbol)

        # retrieve call symbol
        call_optionSymbol = getCallOptionSymbol(content)

        # retrieve put symbol
        put_optionSymbol = getPutOptionSymbol(content)
        
        
        val = symbol.split(" ")
        
        
        # get price history from tradestation
        barsBack = 210
        minutes = 5
        if (len(val) > 1):
            df = getPriceHistory_TS(val[0],str(barsBack), str(minutes))
        else :
            df = getPriceHistory_TS(symbol,str(barsBack), str(minutes))
                   
            
        #convert current candles to heiken-ashi
        df = getHA(df)               
             
        
        ema9 = df['ema9']
        ema50 = df['ema50']
        ema200 = df['ema200']
        atr = df['ATR']
        rsi = df['RSI']
        
        #retrieve keys
        last_key = ema9.keys()[-1]
        previous_key = ema9.keys()[-2]
        previous_key2 = ema9.keys()[-3]
        previous_key3 = ema9.keys()[-4]
        
        
        datetime_now = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        strategy = 'MANUAL_OPEN'
        
        if (optiontype == "BUYEQUITY"):
            
            # Buy Equity
            if (not getSymbolStock_LONG_PositionsfromDB(symbol)) :

                # get quote from tradestation
                quote = getQuote(symbol)
                #midPrice = round((quote['Bid'] + quote['Ask'])/2, 2)
                bidPrice = round((quote['Bid']), 2)
                askPrice = round((quote['Ask']), 2)
                #openPrice = round((df['Open'][last_key]), 2)
                limitprice = askPrice

                equity_bidaskpercent = getEquity_BidAskPercent(symbol,bidPrice,askPrice)
                #print("equity_bidaskpercent:",equity_bidaskpercent, 'for', symbol)   

                if (equity_bidaskpercent <= 1) :
                    if (symbol == "GOOG"):
                            quantity = 2
                    else :
                        quantity=int(3800/(limitprice))

                    print("ALL CONDITIONS FOR BUY STOCK MET FOR ", symbol)

                    totalCost = round((limitprice*quantity),2)
                    openpl = round((quantity * (bidPrice-limitprice)), 2)
                    openpl_percent = round(((openpl/totalCost)*100), 2)
                    mktvalue = mktvalue = round((quantity * bidPrice),2)
                    #triggerprice = round(limitprice + atr[last_key],2)
                    triggerprice = round(quantity * atr[last_key],2)
                    previous_openpl = round((quantity * (bidPrice-limitprice)), 2)
                    profit_target = limitprice * 1.0065
                    stop_loss = limitprice * 0.995

                    #buy Equity in TS
                    #buyEQUITY(symbol, quantity, limitprice, tradelog_filename)

                    winsound.Beep(4000, 500)

                    # add to DB
                    addOrderOutputtoDB(datetime_now, str(symbol), 'BUY_STOCK', limitprice, quantity)
                    addPositionOutputtoDB(datetime_now, str(symbol), 'Long', quantity, limitprice,totalCost, openpl, openpl_percent, bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)
                    
                                
                    #add log to DB
                    addOrderLogOutputtoDB(datetime_now, symbol, strategy,'BUY_STOCK', limitprice, quantity, '', bidPrice, askPrice,df)


            
        if (optiontype == "SELLSHORTEQUITY"): 
            
            # SellShort Equity
            if (not getSymbolStock_SHORT_PositionsfromDB(symbol)) :

                # get quote from tradestation
                quote = getQuote(symbol)
                #midPrice = round((quote['Bid'] + quote['Ask'])/2, 2)
                askPrice = round((quote['Ask']), 2)
                bidPrice = round((quote['Bid']), 2)
                #openPrice = round((df['Open'][last_key]), 2)
                limitprice = bidPrice
                equity_bidaskpercent = getEquity_BidAskPercent(symbol,bidPrice,askPrice)
                #print("equity_bidaskpercent:",equity_bidaskpercent, 'for', symbol)

                if (equity_bidaskpercent <= 1) :

                    if (symbol == "GOOG") :
                            quantity = 2
                    else :
                        quantity=int(3800/(limitprice))

                    print("ALL CONDITIONS FOR SELLSHORT MET FOR ", symbol)

                    totalCost = round((limitprice*quantity),2)
                    openpl = round((quantity * (limitprice-askPrice)), 2)
                    openpl_percent = round(((openpl/totalCost)*100), 2)
                    mktvalue = round((quantity * bidPrice),2)
                    #triggerprice = round(limitprice - atr[last_key_2min],2)   
                    triggerprice = round(quantity * atr[last_key],2)
                    previous_openpl = round((quantity * (limitprice-askPrice)), 2)
                    profit_target = limitprice * 0.935
                    stop_loss = limitprice * 1.005

                    #sellshort Equity in TS
                    #sellshortEQUITY(symbol, quantity, limitprice, tradelog_filename)

                    winsound.Beep(4000, 500)

                    #add to DB
                    addOrderOutputtoDB(datetime_now, str(symbol), 'SELLSHORT_STOCK', limitprice, quantity)
                    addPositionOutputtoDB(datetime_now, str(symbol), 'Short', quantity, limitprice, totalCost, openpl,openpl_percent,bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)
                                        
                                        
                    #add log to DB
                    addOrderLogOutputtoDB(datetime_now, symbol, strategy,'SELLSHORT_STOCK', limitprice, quantity, '', bidPrice, askPrice,df)

            
       
        if (optiontype == "CALL"): 
            if (not getSymbolCallPositionsfromDB(symbol)) :
                
                bidaskpercent = getBidAskPercent(content, "callExpDateMap")
                print("bidaskpercent:",bidaskpercent,'% for', symbol)
                if bidaskpercent <= 5 :
                    askPrice = getAskPrice(content, "callExpDateMap")
                    bidPrice = getBidPrice(content, "callExpDateMap")
                    limitprice = askPrice
                    print('limit',limitprice)

                    if (symbol == "TSLA") or (symbol == "GOOG") :
                        quantity = 1
                    else :
                        quantity=int(3800/(limitprice*100))
                    
                    print("ALL CONDITIONS FOR BUY CALL OPTION MET FOR ", symbol)

                    totalCost = round((limitprice*quantity*100),2)
                    openpl = round((quantity * 100* (bidPrice-limitprice)), 2)
                    openpl_percent = round(((openpl/totalCost)*100), 2)
                    mktvalue = mktvalue = round((quantity * 100 * bidPrice),2)
                    #triggerprice = round(limitprice + atr[last_key],2)
                    triggerprice = round(quantity * atr[last_key],2)
                    previous_openpl = round((quantity * 100 * (bidPrice-limitprice)), 2)
                    profit_target = limitprice * 1.0065
                    stop_loss = limitprice * 0.995
                    
                    winsound.Beep(4000, 500)
                    
                    # add to DB
                    addOrderOutputtoDB(datetime_now, str(call_optionSymbol), 'BUY_CALL', limitprice, quantity)
                    addPositionOutputtoDB(datetime_now, str(call_optionSymbol), 'Long', quantity, limitprice,totalCost, openpl, openpl_percent, bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)
                    
                                
                    #add log to DB
                    addOrderLogOutputtoDB(datetime_now, str(call_optionSymbol), strategy,'BUY_CALL', limitprice, quantity, '', bidPrice, askPrice,df)
                    


        if (optiontype == "PUT"):             
            if (not getSymbolPutPositionsfromDB(symbol)) :
                
                bidaskpercent = getBidAskPercent(content, "putExpDateMap")
                print("bidaskpercent:",bidaskpercent,'% for', symbol)
                if bidaskpercent <= 5 :
                    askPrice = getAskPrice(content, "putExpDateMap")
                    bidPrice = getBidPrice(content, "putExpDateMap")
                    limitprice = askPrice
                    print('limit',limitprice)

                    if (symbol == "TSLA") or (symbol == "GOOG") :
                        quantity = 1
                    else :
                        quantity=int(3800/(limitprice*100))
                    
                    print("ALL CONDITIONS FOR BUY PUT OPTION MET FOR ", symbol)

                    totalCost = round((limitprice*quantity*100),2)
                    openpl = round((quantity * 100* (bidPrice-limitprice)), 2)
                    openpl_percent = round(((openpl/totalCost)*100), 2)
                    mktvalue = mktvalue = round((quantity * 100 * bidPrice),2)
                    #triggerprice = round(limitprice + atr[last_key],2)
                    triggerprice = round(quantity * atr[last_key],2)
                    previous_openpl = round((quantity * 100 * (bidPrice-limitprice)), 2)
                    profit_target = limitprice * 1.0065
                    stop_loss = limitprice * 0.995
                    
                    winsound.Beep(4000, 500)
                    
                    # add to DB
                    addOrderOutputtoDB(datetime_now, str(put_optionSymbol), 'BUY_PUT', limitprice, quantity)
                    addPositionOutputtoDB(datetime_now, str(put_optionSymbol), 'Long', quantity, limitprice,totalCost, openpl, openpl_percent, bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss)
                    
                                
                    #add log to DB
                    addOrderLogOutputtoDB(datetime_now, str(put_optionSymbol), strategy,'BUY_PUT', limitprice, quantity, '', bidPrice, askPrice,df)
                    
        
    except Exception as e:
        traceback.print_exc()
        print(e)


# sell all positions from AE simulated account
def closeAllPositionsBeforeMarketClose(position):
    
    try:

        symbol = position['SYMBOL']
        positionType = position['POSITION_TYPE']
        quantity =position['QUANTITY']
        avgPrice = position['AVGPRICE']
        bidPrice = position['BID']
        askPrice = position['ASK']
        openpl_percent = position['OPENPL_PERCENT']
        limitprice = round((askPrice + bidPrice)/2,2)
        
        val = symbol.split(" ")

        # if the position is an option then sell according to the rules below
        if (len(val) > 1):
            if (val[1].__contains__("C")):
                print (datetime.now().strftime('%H:%M:%S'))
                print('Sell Call for ', symbol, 'at', limitprice)
                addOrderOutputtoCSV(orderlog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'SELL_CALL', limitprice, quantity)
                profitloss = round((quantity * 100 * (limitprice-avgPrice)),2)
                addprofitlossOutputtoCSV(balancelog_filename, profitloss)
                deletePositionfromCSV(positionlog_filename, symbol)
                
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'SELL_CALL', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)


            if (val[1].__contains__("P")):
                print (datetime.now().strftime('%H:%M:%S'))
                print('Sell Put for ', symbol, 'at', limitprice)
                addOrderOutputtoCSV(orderlog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'SELL_PUT', limitprice, quantity)
                profitloss = round((quantity * 100 * (limitprice-avgPrice)),2)
                addprofitlossOutputtoCSV(balancelog_filename, profitloss)
                deletePositionfromCSV(positionlog_filename, symbol)
                
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'SELL_PUT', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)

        # if the position is a stock then sell according to the rules below
        else :
            if (positionType == 'Long'):
                print (datetime.now().strftime('%H:%M:%S'))
                print('Sell Stock for ', symbol, 'at', limitprice)
                addOrderOutputtoCSV(orderlog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'SELL_STOCK', limitprice, quantity)
                profitloss = round((quantity * (limitprice-avgPrice)),2)
                addprofitlossOutputtoCSV(balancelog_filename, profitloss)
                deletePositionfromCSV(positionlog_filename, symbol)
                
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'SELL_STOCK', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)
                    
            if (positionType == 'Short'):        
                print (datetime.now().strftime('%H:%M:%S'))
                print('Buy to Cover for ', symbol, 'at', limitprice)
                addOrderOutputtoCSV(orderlog_filename, datetime.now().strftime('%D %H:%M:%S'), str(symbol), 'BUYTOCOVER_STOCK', limitprice, quantity)
                profitloss = round((quantity * (avgPrice-limitprice)),2)
                addprofitlossOutputtoCSV(balancelog_filename, profitloss)
                deletePositionfromCSV(positionlog_filename, symbol)
                
                #add to DB
                addOrderOutputtoDB(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(symbol), 'BUYTOCOVER_STOCK', limitprice, quantity)
                addprofitlossOutputtoDB(profitloss)
                deletePositionfromDB(symbol)
            

        #print("******************")
        
        
    except Exception as e:
        traceback.print_exc()
        print(e)
        pass
    
def copyFile(source,destination):
    import os
    import shutil

    # Copy the content of source to destination
    dest = shutil.copyfile(source, destination)

def is_locked(filepath):
    """Checks if a file is locked by opening it in append mode.
    If no exception thrown, then the file is not locked.
    """
    locked = False
    file_object = None
    if os.path.exists(filepath):
        try:
            #print ("Trying to open %s." % filepath)
            buffer_size = 8
            # Opening file in append mode and read the first 8 characters.
            file_object = open(filepath, 'a', buffer_size)
            if file_object:
                #print ("%s is not locked." % filepath)
                locked = False
        except IOError:
            print ("File is locked (unable to open in append mode)" )
            locked = True
        finally:
            if file_object:
                file_object.close()
                #print ("%s closed." % filepath)
    else:
        print ("%s not found." % filepath)
    return locked     



def lock(file):
    try:
        if os.name == 'nt':
            import win32con, win32file, pywintypes
            LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
            LOCK_SH = 0 # the default
            LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
            _overlapped = pywintypes.OVERLAPPED(  )
            hfile = win32file._get_osfhandle(file.fileno(  ))
            win32file.LockFileEx(hfile, LOCK_SH, 0, 0xffff0000, _overlapped)
    except Exception as e:
        traceback.print_exc()
        print(e)
            

def unlock(file):
    try:
        if os.name == 'nt':
            import win32con, win32file, pywintypes
            LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
            LOCK_SH = 0 # the default
            LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
            _overlapped = pywintypes.OVERLAPPED(  )
            hfile = win32file._get_osfhandle(file.fileno(  ))
            win32file.UnlockFileEx(hfile, 0, 0xffff0000, _overlapped)
    except Exception as e:
        traceback.print_exc()
        print(e)
        
def removeStockfromFile(symbol,filename):
    
    symbol_list = pd.read_csv(filename, header=None)[0].tolist()
    
    symbol_list.remove(symbol)
    print(symbol_list)
    
    df = pd.DataFrame(symbol_list)
    
    print(df)
    
    df.to_csv('longstocks.csv', index=False,  header=False)

    
import MySQLDB
from MySQLDB import *

def getDBConnection():
    db,cursor=MySQLPool().getConnection()
    return db,cursor

#def getDBConnection():
    
#    db = mysql.connector.connect(host="localhost",user="admin",password="admin",database="alphaedge")
#    cursor = db.cursor(buffered=True)
#    return db,cursor


def addTradeSignalsOutputtoDB(date_time, symbol, timeframe, bid, ask, strategy, trade_signal):
    try:
        #get MySQL DB Cursor
        db,conn=getDBConnection()
        #sql = "INSERT INTO positions (SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,BID,ASK,OPENPL,OPENPL_PERCENT,MKTVALUE) \
        #        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql = "INSERT INTO trade_signals (DATETIME,SYMBOL,TIMEFRAME,BID,ASK,STRATEGY,TRADE_SIGNAL) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        vals=(date_time, symbol, timeframe, bid, ask, strategy, trade_signal)
        conn.execute(sql,vals)
        db.commit()
        db.close()
        return conn.rowcount
    except Exception as e:
        traceback.print_exc()
        print(e)
        
        
def addOrderOutputtoDB(date_time, optionSymbol, trade, limitprice, quantity):
    try:
        #get MySQL DB Cursor
        db,conn=getDBConnection()
        #sql = "INSERT INTO positions (SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,BID,ASK,OPENPL,OPENPL_PERCENT,MKTVALUE) \
        #        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql = "INSERT INTO orders (DATETIME,SYMBOL,TRADE,LIMIT_PRICE,QUANTITY) VALUES (%s,%s,%s,%s,%s)"
        vals=(date_time, optionSymbol, trade, limitprice, quantity)
        conn.execute(sql,vals)
        db.commit()
        db.close()
        return conn.rowcount
    except Exception as e:
        traceback.print_exc()
        print(e)
        
        
        
        
def addPositionOutputtoDB(date_time, positionsymbol, positiontype, quantity, limitprice,totalCost, openpl,openpl_percent,bidPrice,askPrice,mktvalue, triggerprice,previous_openpl,strategy,profit_target,stop_loss):
   
    try:
        #get MySQL DB Cursor
        db,conn=getDBConnection()
        
        sql = "INSERT INTO positions (DATETIME, SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,BID,ASK,OPENPL,OPENPL_PERCENT,MKTVALUE,TRIGGER_PRICE,PREVIOUS_OPENPL,STRATEGY,PROFIT_TARGET,STOP_LOSS) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        vals=(date_time, positionsymbol, positiontype, quantity, limitprice, totalCost, bidPrice ,askPrice, openpl, openpl_percent, str(mktvalue), str(triggerprice), str(previous_openpl),str(strategy),profit_target,stop_loss)
        conn.execute(sql,vals)
        db.commit()
        db.close()
        return conn.rowcount
    except Exception as e:
        traceback.print_exc()
        print(e)

        
def addprofitlossOutputtoDB(profitloss):
    try:
        db,conn=getDBConnection()
        now=datetime.now().strftime('%Y-%m-%d')
        profit_loss_sql="select PROFIT_LOSS from balances where DATE='" + now + "'"
        conn.execute(profit_loss_sql)
        results = conn.fetchall()
        current_pl=0
        total_pl=0
        for row in results:
            current_pl=row[0]
            total_pl=current_pl+profitloss
        
        if(total_pl==0):
            sql="insert into balances (DATE,PROFIT_LOSS) values (%s,%s)"
            vals=(now,profitloss)
            conn.execute(sql,vals)
            #print(sql)
        else:
            sql = "update balances SET PROFIT_LOSS='" + str(total_pl) + "' where DATE='" + now + "'"
            conn.execute(sql)
            #print(sql)
            

        db.commit()
        db.close()
        return conn.rowcount        
    except Exception as e:
        traceback.print_exc()
        print(e)


def getDFFromPositionTable():
    try:
        
        db,conn=getDBConnection()
        sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE,TRIGGER_PRICE,PREVIOUS_OPENPL,STRATEGY,PROFIT_TARGET,STOP_LOSS from positions"
        conn.execute(sql)
                
        df_positions = pd.read_sql(sql,db)        
        positions = df_positions.to_dict('records')
       
        db.commit()
        db.close()
        return positions

    except Exception as e:
        traceback.print_exc()
        print(e)
        
        
def updateTotalCostBasis():
    try:
               
        db,conn=getDBConnection()
        sql = "select sum(TOTALCOST) from positions"  
        conn.execute(sql)
        total_cost_sum=0.0;
        results = conn.fetchall()
        for row in results:            
            total_cost_sum = row[0]
        #print("Sum of total_cost=", str(total_cost_sum))
        
        sql = "select TOTAL_COST_BASIS, PROFIT_LOSS,PROFIT_LOSS_PERCENT from BALANCES"  
        conn.execute(sql)
        total_cost_basis=0.0;
        profit_loss_percent = 0
        results = conn.fetchall()
        for row in results:            
            total_cost_basis = row[0]
            profit_loss = row[1]
            profit_loss_percent = row[2]
            
            if (total_cost_basis is None):
                total_cost_basis =0.0
                profit_loss_percent = 0
        #print("Total_cost_basis=", str(total_cost_basis))
        
        if (total_cost_basis != 0):
            profit_loss_percent = round(float((profit_loss/total_cost_basis) * 100),2)
        
        now=datetime.now().strftime('%Y-%m-%d')
        sql = "update BALANCES set PROFIT_LOSS_PERCENT="+str(profit_loss_percent)+ " where DATE='" + now + "'"
        conn.execute(sql)
        
        
        if (total_cost_sum != None) :            
            if(total_cost_basis<total_cost_sum):
                sql = "update BALANCES set TOTAL_COST_BASIS="+str(total_cost_sum)+ " where DATE='" + now + "'"
                conn.execute(sql)
        
        db.commit()
        
        db.close()
           
    except Exception as e:
        traceback.print_exc()
        print(e)
    
    
def updatePositionValuesinDB():
    try:
               
        db,conn=getDBConnection()
        sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,BID,ASK,OPENPL,OPENPL_PERCENT,MKTVALUE,TRIGGER_PRICE,PREVIOUS_OPENPL,STRATEGY,PROFIT_TARGET,STOP_LOSS from positions"
        conn.execute(sql)
        results = conn.fetchall()
        #print(results)
        
        for row in results:            
            symbol = row[0]
            positionType = row[1]
            quantity = row[2]
            avgPrice = row[3]
            totalCost = row[4]
            bidPrice = row[5]
            askPrice = row[6]
            openpl = row[7]
            openpl_percent = row[8]
            mktvalue = row[9]
            triggerprice = row[10] 
            previous_openpl = row[11]
            strategy = row[12]
            profit_target = row[13]
            stop_loss = row[14]
                        
            
            quote=getQuote_TS(symbol)
            bidPrice = quote['Bid']
            askPrice = quote['Ask']
            previous_openpl = openpl
            #quote=getQuote_YF(symbol)
            #bidPrice = quote['bid']
            #askPrice = quote['ask']
            
            
            val = symbol.split(" ")
            
            #update value for options
            if (len(val) > 1):
                
                totalCost = round((avgPrice*100*quantity),2)
                mktvalue = round((quantity * 100 * bidPrice),2)
                openpl = round((quantity * 100 * (bidPrice-avgPrice)), 2)
                openpl_percent = round(((openpl/totalCost)*100), 2)
                
                '''
                if (float(bidPrice) > float(profit_target)):
                        #stop_loss = profit_target - float((triggerprice * 0.50)/quantity)
                        stop_loss = profit_target - float((openpl * 0.40)/quantity) 
                        if (stop_loss < avgPrice) :
                            stop_loss = avgPrice                        
                        #profit_target = profit_target + float((triggerprice * 0.50)/quantity) 
                        profit_target = bidPrice
                '''
                
            #update value for equities
            else :
                if (positionType == 'Long') :                        
                    totalCost = round((avgPrice*quantity),2)
                    mktvalue = round((quantity * bidPrice),2)
                    openpl = round((quantity * (bidPrice-avgPrice)), 2)
                    openpl_percent = round(((openpl/totalCost)*100), 2)
                    
                    #if (openpl_percent >= 1.00) :
                        #stop_loss = avgPrice * 1.01 
                        
                    
                    '''
                    if (float(bidPrice) > float(profit_target)):
                        #stop_loss = profit_target - float((triggerprice * 0.50)/quantity)
                        stop_loss = profit_target - float((openpl * 0.40)/quantity) 
                        if (stop_loss < avgPrice) :
                            stop_loss = avgPrice                        
                        #profit_target = profit_target + float((triggerprice * 0.50)/quantity) 
                        profit_target = bidPrice
                    '''    
                        

                if (positionType == 'Short') :
                    totalCost = round((avgPrice*quantity),2)
                    mktvalue = round((quantity * bidPrice),2)
                    openpl = round((quantity * (avgPrice-askPrice)), 2)
                    openpl_percent = round(((openpl/totalCost)*100), 2)
                    
                    #if (openpl_percent >= 1.00) :
                        #stop_loss = avgPrice * 0.99 
                    
                    '''
                    if (float(askPrice) < float(profit_target)):
                        #stop_loss = profit_target + float((triggerprice * 0.50)/quantity)
                        stop_loss = profit_target + float((openpl * 0.20)/quantity)
                        if (stop_loss > avgPrice) :
                            stop_loss = avgPrice
                            
                        #profit_target = float(profit_target) * 0.935
                        #profit_target = profit_target - float((triggerprice * 0.50)/quantity) 
                        profit_target = askPrice  
                   '''

                    

            sql="update positions SET TOTALCOST='" + str(totalCost) + "', BID='" + str(bidPrice) + "',ASK='"+ str(askPrice) + "',OPENPL='"+ str(openpl) + "',OPENPL_PERCENT='"+ str(openpl_percent) + "',MKTVALUE='" + str(mktvalue) + "',PREVIOUS_OPENPL='" + str(previous_openpl) + "',PROFIT_TARGET='" + str(profit_target) + "',STOP_LOSS='" + str(stop_loss) + "' where SYMBOL='" + str(symbol) + "' and POSITION_TYPE ='" + str(positionType) + "' and format(AVGPRICE,2)= format(" + str(avgPrice) + ",2)"
            #print("Update sql: ", sql)
           
            conn.execute(sql)
            db.commit()
        
        db.close()
   
    except Exception as e:
        traceback.print_exc()
        print(e)
        
        
        
def deletePositionfromDB(symbol):
    try:
        db,conn=getDBConnection()
        sql="delete from positions where SYMBOL='" + symbol + "'"
        #print("Delete sql: ", sql)
        conn.execute(sql)
        db.commit()
        db.close()
    except Exception as e:
        traceback.print_exc()
        print(e)
        
        
def getSymbolCallPositionsfromDB(symbol):
    
    db,conn=getDBConnection()
    sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE from positions"
    conn.execute(sql)
                
    df_positions = pd.read_sql(sql,db) 
        
    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]

        val1 = val.split(" ")
        if (len(val1) > 1):
            if ((val1[0] == symbol) and val1[1].__contains__("C")):
                print('CallPositionFound', val)
                return True
    return False
    


def getSymbolPutPositionsfromDB(symbol):
    db,conn=getDBConnection()
    sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE from positions"
    conn.execute(sql)
                
    df_positions = pd.read_sql(sql,db) 
    
    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]

        val1 = val.split(" ")
        if (len(val1) > 1):
            if ((val1[0] == symbol) and val1[1].__contains__("P")):
                print('PutPositionFound', val)
                return True
    return False

def getSymbolStock_LONG_PositionsfromDB(symbol):
    db,conn=getDBConnection()
    sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE from positions"
    conn.execute(sql)
                
    df_positions = pd.read_sql(sql,db)        

    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]
        position_type = df_positions['POSITION_TYPE'][ind]
        if ((val == symbol) and (position_type == 'Long')) :
            return True
    return False


def getSymbolStock_SHORT_PositionsfromDB(symbol):
    db,conn=getDBConnection()
    sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE from positions"
    conn.execute(sql)
                
    df_positions = pd.read_sql(sql,db)        

    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]
        position_type = df_positions['POSITION_TYPE'][ind]
        if ((val == symbol) and (position_type == 'Short')) :
            return True
    return False


def getSymbolStockPositionsfromDB(symbol):
    db,conn=getDBConnection()
    sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE from positions"
    conn.execute(sql)
                
    df_positions = pd.read_sql(sql,db)        

    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]
        if (val == symbol):
            return True
    return False

'''
def getSymbolStockPositionsfromDB(symbol,strategy):
    db,conn=getDBConnection()
    sql = "select SYMBOL,POSITION_TYPE,QUANTITY,AVGPRICE,TOTALCOST,OPENPL,OPENPL_PERCENT,BID,ASK,MKTVALUE from positions where strategy ='" + strategy + "'"
    print(sql)
    conn.execute(sql)
                
    df_positions = pd.read_sql(sql,db)        

    for ind in df_positions.index:    
        val = df_positions['SYMBOL'][ind]
        if (val == symbol):
            return True
    return False
'''

def updateSymbolDataFrameOutputtoDB(symbol,df):
   
    try:
        ema9 = df['ema9']
        

        last_key = ema9.keys()[-1]
        previous_key = ema9.keys()[-2]
        
        close_lk = df['Close'][last_key]
        open_lk = df['Open'][last_key]
        high_lk = df['High'][last_key]
        low_lk = df['Low'][last_key]
        
        close_pk = df['Close'][previous_key]
        open_pk = df['Open'][previous_key]
        high_pk = df['High'][previous_key]
        low_pk = df['Low'][previous_key]
        
        
        
        df['TimeStamp'][previous_key] = datetime.strftime(datetime.strptime(df['TimeStamp'][previous_key],'%m-%d-%Y %H:%M:%S %p'),'%Y-%m-%d %H:%M:%S')
        datetime_pk = df['TimeStamp'][previous_key]
       
        #get MySQL DB Cursor
        
        print(datetime_pk)
        
        db,conn=getDBConnection()
        sql = "DELETE from symbol_dataframe where symbol='"+symbol + "'"
        conn.execute(sql)
        sql="INSERT INTO  symbol_dataframe (DATETIME,SYMBOL, OPEN,HIGH,LOW,CLOSE) values (%s,%s,%s,%s,%s,%s)"
        vals=(datetime_pk,symbol,float(open_pk),float(high_pk),float(low_pk),float(close_pk))
        conn.execute(sql,vals)
        #sql="INSERT INTO  symbol_dataframe (DATETIME,SYMBOL, OPEN,HIGH,LOW,CLOSE) values (%s,%s,%s,%s,%s)"
        #vals=(last_key,open_lk,high_lk,low_lk,close_lk)
        #conn.execute(sql,vals)
        db.commit()
        db.close()
        return conn.rowcount
    except Exception as e:
        traceback.print_exc()
        print(e)

        
#ADD LOGS TO ORDER_LOG TABLE for orders logging only        
def addOrderLogOutputtoDB(date_time, symbol, strategy,order_type, limitprice, quantity, profit_loss, bid, ask,df):
    try:
        
        ema9 = df['ema9']
        ema21 = df['ema21']
        ema50 = df['ema50']
        stddev = df['stddeviation']
        vwap = df['vwap']
        upper = df['upper_bb']
        middle = df['middle_bb']
        lower = df['lower_bb']
        atr=df['ATR']
        rsi=df['RSI']
        up_volume=df['UpVolume']
        down_volume=df['DownVolume']
        kfast = df['K_fast']
        dfast = df['D_fast']
        kslow = df['K_slow']
        dslow = df['D_slow']
                       

        last_key = ema9.keys()[-1]
        previous_key = ema9.keys()[-2]
        previous_key2 = ema9.keys()[-3]
           
              
        
        #get MySQL DB Cursor
        db,conn=getDBConnection()
        
        sql = "INSERT INTO orders_log(DATETIME, SYMBOL, STRATEGY, ORDER_TYPE, LIMIT_PRICE, QUANTITY, PROFIT_LOSS, BID, ASK, OPEN, HIGH, LOW, CLOSE, EMA9, EMA21, EMA50, VWAP, UPPER_BB, LOWER_BB, STD_DEV, ATR, RSI, UP_VOLUME, DOWN_VOLUME,PREVIOUS2_KFAST, PREVIOUS_KFAST, PREVIOUS2_DFAST, PREVIOUS_DFAST,PREVIOUS2_KSLOW, PREVIOUS_KSLOW, PREVIOUS2_DSLOW, PREVIOUS_DSLOW) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        vals=(date_time, symbol, strategy,order_type, str(limitprice), str(quantity),str(profit_loss),str(bid),str(ask),str(df['Open'][previous_key]),str(df['High'][previous_key]),str(df['Low'][previous_key]),str(df['Close'][previous_key]),str(ema9[previous_key]),str(ema21[previous_key]),str(ema50[previous_key]),str(vwap[previous_key]),str(upper[previous_key]),str(lower[previous_key]),str(stddev[previous_key]),str(atr[previous_key]),str(rsi[previous_key]),str(up_volume[previous_key]),str(down_volume[previous_key]),str(kfast[previous_key2]),str(kfast[previous_key]),str(dfast[previous_key2]),str(dfast[previous_key]),str(kslow[previous_key2]),str(kslow[previous_key]),str(dslow[previous_key2]),str(dslow[previous_key]))
              
        #print(sql)
        #print(vals)
        
        conn.execute(sql,vals)
        db.commit()
        db.close()
        return conn.rowcount
              
    except Exception as e:
        traceback.print_exc()
        print(e)

        
        
def addOrderLogOutputtoDB_noDF(date_time, symbol, strategy,order_type, limitprice, quantity, profit_loss, bid, ask):
    try:
        
        
        
        #get MySQL DB Cursor
        db,conn=getDBConnection()
        
        sql = "INSERT INTO orders_log(DATETIME, SYMBOL, STRATEGY, ORDER_TYPE, LIMIT_PRICE, QUANTITY, PROFIT_LOSS, BID, ASK) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        vals=(date_time, symbol, strategy,order_type, str(limitprice), str(quantity),str(profit_loss),str(bid),str(ask))
              
        #print(sql)
        #print(vals)
        
        conn.execute(sql,vals)
        db.commit()
        db.close()
        return conn.rowcount
              
    except Exception as e:
        traceback.print_exc()
        print(e)
        
        

        
# creating sum_list function
def sumOfList(list, size):
   if (size == 0):
     return 0
   else:
     return list[size - 1] + sumOfList(list, size - 1)

    

#reset AlphaEdge Simulated Account Positions, Orders and Balances from the database
def resetPositionsOrdersBalancesfromDB() :
    
    db,conn=getDBConnection()
    sql = "DELETE from positions"
    conn.execute(sql)
    sql = "DELETE from orders"
    conn.execute(sql)
    sql = "DELETE from balances"
    conn.execute(sql)

    db.commit()
    db.close()

    
    print ('All Contents from Position, Order and Balance Database deleted')
    

#Volume Rate of Chg: VolROC,%Chg: %Chg
# get Momentum Stocks from Tradestation Scanner - Not used right now
def getVolumeMomentumStocks_TS_Scanner():
    
    #path='C:\\Users\\manpr\\OneDrive\\Documents\\TradeStation 10.0\\Scans\\Scanner\\TT.tsscan\\*.txt'
    
    #path= 'C:\\Users\\alpit\\Documents\\TradeStation 10.0\\Scans\\_root\\VolumeMomentum.tsscan\\*.txt'
    path='C:\\Users\\VivekP\\Documents\\TradeStation 10.0\\Scans\\_root\\Momentum.tsscan\\*.txt'
    list_of_files = glob.glob(path)
    latest_scan_file = max(list_of_files, key=os.path.getctime)
    #print(latest_scan_file)
    with open(latest_scan_file,encoding='utf-16') as latest_scan_file:
        df_Momentum=pd.read_csv(latest_scan_file)
        df_Momentum.dropna(axis=0, how='any', inplace=True)
        #df=dataframe.rename(columns={'Momentum: Accel': 'Accel','Momentum: Momentum': 'Momentum'},inplace=True)
        if ('Rate of Chg: ROC' in df_Momentum.columns):
            sorted_df=df_Momentum.sort_values(by=['Rate of Chg: ROC','Volume Brkout: VolBrkOut'],ascending=False)
            return sorted_df
        
        else:
            return pd.DataFrame()


# get Momentum Stocks from Tradestation Scanner
def getMomentumStocks_TS_Scanner():
    
    try:
        #path='C:\\Users\\manpr\\OneDrive\\Documents\\TradeStation 10.0\\Scans\\Scanner\\TT.tsscan\\*.txt'
    
        path= 'C:\\Users\\alpit\\Documents\\TradeStation 10.0\\Scans\\_root\\Momentum.tsscan\\*.txt'

        list_of_files = glob.glob(path)    
        latest_scan_file = max(list_of_files, key=os.path.getctime)
        #print(latest_scan_file)
        with open(latest_scan_file,encoding='utf-16') as latest_scan_file:
            df_Momentum=pd.read_csv(latest_scan_file)
            df_Momentum.dropna(axis=0, how='any', inplace=True)
            #df=dataframe.rename(columns={'Momentum: Accel': 'Accel','Momentum: Momentum': 'Momentum'},inplace=True)

            sorted_df=df_Momentum.sort_values(by=['Rate of Chg: ROC'],ascending=False)

        return sorted_df

    except Exception as e:
            traceback.print_exc()
            print(e)
            pass
        
        
        
 # get Momentum Stocks from Tradestation Scanner
def getNewMomoStocks_TS_Scanner():
    
    try:
        #path='C:\\Users\\manpr\\OneDrive\\Documents\\TradeStation 10.0\\Scans\\Scanner\\TT.tsscan\\*.txt'
    
        path= 'C:\\Users\\alpit\\Documents\\TradeStation 10.0\\Scans\\_root\\New_Momo.tsscan\\*.txt'

        list_of_files = glob.glob(path)    
        latest_scan_file = max(list_of_files, key=os.path.getctime)
        #print(latest_scan_file)
        with open(latest_scan_file,encoding='utf-16') as latest_scan_file:
            df_Momentum=pd.read_csv(latest_scan_file)
            df_Momentum.dropna(axis=0, how='any', inplace=True)
            #df=dataframe.rename(columns={'Momentum: Accel': 'Accel','Momentum: Momentum': 'Momentum'},inplace=True)

            sorted_df=df_Momentum.sort_values(by=['Rate of Chg: ROC'],ascending=False)

        return sorted_df

    except Exception as e:
            traceback.print_exc()
            print(e)
            pass
        
        
def Supertrend(df, atr_period, multiplier):
   
    #heiken-ashi candles
    high = df['HA_High']
    low = df['HA_Low']
    close = df['HA_Close']
   
    # calculate ATR
    price_diffs = [high - low,
                   high - close.shift(),
                   close.shift() - low]
    true_range = pd.concat(price_diffs, axis=1)
    true_range = true_range.abs().max(axis=1)
    # default ATR calculation in supertrend indicator
    atr = true_range.ewm(alpha=1/atr_period,min_periods=atr_period).mean()
    # df['atr'] = df['tr'].rolling(atr_period).mean()
   
    # HL2 is simply the average of high and low prices
    hl2 = (high + low) / 2
    # upperband and lowerband calculation
    # notice that final bands are set to be equal to the respective bands
    final_upperband = upperband = hl2 + (multiplier * atr)
    final_lowerband = lowerband = hl2 - (multiplier * atr)
   
    # initialize Supertrend column to True
    supertrend = [True] * len(df)
   
    for i in range(1, len(df.index)):
        curr, prev = i, i-1
       
        # if current close price crosses above upperband
        if close[curr] > final_upperband[prev]:
            supertrend[curr] = True
        # if current close price crosses below lowerband
        elif close[curr] < final_lowerband[prev]:
            supertrend[curr] = False
        # else, the trend continues
        else:
            supertrend[curr] = supertrend[prev]
            # adjustment to the final bands
            if supertrend[curr] == True and final_lowerband[curr] < final_lowerband[prev]:
                final_lowerband[curr] = final_lowerband[prev]
            if supertrend[curr] == False and final_upperband[curr] > final_upperband[prev]:
                final_upperband[curr] = final_upperband[prev]

        # to remove bands according to the trend direction
        if supertrend[curr] == True:
            final_upperband[curr] = np.nan
        else:
            final_lowerband[curr] = np.nan
       

    df['Supertrend']=supertrend
    df['ST_Lowerband']=final_lowerband
    df['ST_Upperband']=final_upperband
   
    return df


 
def UTBot_Buy_OLD(df,sensitivity,atr_period,ha_signals):

    last_key=df['HA_Close'].keys()[-1]
    previous_key=df['HA_Close'].keys()[-2]
    previous_key_2=df['HA_Close'].keys()[-3]
    
    
    df=calculate_period_atr(df,atr_period)
    xATR=df['ATR']
    
    nLoss = pd.Series(len(df['HA_Close']))                              
    
    nLoss = sensitivity * xATR
    
    if (ha_signals):
        df = getHA(df)
   
    xATRTrailingStop = pd.Series(len(df['HA_Close']))
    
    for i in range(len(df['HA_Close'])-1) :                                 
        xATRTrailingStop[i]=0.0
    
   
    df['utbot_signal_BUY'] = "" 
    for i in range(len(df['HA_Close'])-1):

        buy = False
        sell = False    


        if(df['HA_Close'][i+1] > xATRTrailingStop[i] and df['HA_Close'][i] > xATRTrailingStop[i]):
            xATRTrailingStop[i+1]=max(xATRTrailingStop[i],df['HA_Close'][i+1] - nLoss[i+1])
        elif (df['HA_Close'][i+1] < xATRTrailingStop[i] and df['HA_Close'][i] < xATRTrailingStop[i]):
            xATRTrailingStop[i+1]=min(xATRTrailingStop[i],df['HA_Close'][i+1] + nLoss[i+1])
        elif(df['HA_Close'][i+1] > xATRTrailingStop[i]):
            xATRTrailingStop[i+1]=df['HA_Close'][i+1] - nLoss[i+1]
        else:
            xATRTrailingStop[i+1]=df['HA_Close'][i+1] + nLoss[i+1]

        if(df['HA_Close'][i+1]>xATRTrailingStop[i+1] and df['HA_Close'][i]<=xATRTrailingStop[i]):
            buy=True
            df['utbot_signal_BUY'][i+1] =  "BUY"
            
        #if(df['HA_Close'][i+1]<xATRTrailingStop[i+1] and df['HA_Close'][i]>=xATRTrailingStop[i]):
            #sell=True
            #df['utbot_signal_SELL'][i+1] =  "SELL"
        else :
            df['utbot_signal_BUY'][i+1] = "" 
    
    df['xATRTrailingStop'] = xATRTrailingStop
       
    
    '''
    if(df['HA_Close'][last_key]>xATRTrailingStop[last_key] and df['HA_Close'][previous_key]<=xATRTrailingStop[previous_key]):
        buy=True
        df['utbot_signal_',str(atr_period)][last_key] =  "Buy"
    elif(df['HA_Close'][last_key]<xATRTrailingStop[last_key] and df['HA_Close'][previous_key]>=xATRTrailingStop[previous_key]):
        sell=True
        df['utbot_signal_',str(atr_period)][last_key] =  "Sell"
    '''
    
    return df

def UTBot_Sell_OLD(df,sensitivity,atr_period,ha_signals):

    last_key=df['HA_Close'].keys()[-1]
    previous_key=df['HA_Close'].keys()[-2]
    previous_key_2=df['HA_Close'].keys()[-3]
    
    
    df=calculate_period_atr(df,atr_period)
    xATR=df['ATR']
    
    nLoss = pd.Series(len(df['HA_Close']))                              
    
    nLoss = sensitivity * xATR
    
    if (ha_signals):
        df = getHA(df)
   
    xATRTrailingStop = pd.Series(len(df['HA_Close']))
    
    for i in range(len(df['HA_Close'])-1) :                                 
        xATRTrailingStop[i]=0.0
    
   
    df['utbot_signal_SELL'] = "" 
    for i in range(len(df['HA_Close'])-1):

        buy = False
        sell = False    


        if(df['HA_Close'][i+1] > xATRTrailingStop[i] and df['HA_Close'][i] > xATRTrailingStop[i]):
            xATRTrailingStop[i+1]=max(xATRTrailingStop[i],df['HA_Close'][i+1] - nLoss[i+1])
        elif (df['HA_Close'][i+1] < xATRTrailingStop[i] and df['HA_Close'][i] < xATRTrailingStop[i]):
            xATRTrailingStop[i+1]=min(xATRTrailingStop[i],df['HA_Close'][i+1] + nLoss[i+1])
        elif(df['HA_Close'][i+1] > xATRTrailingStop[i]):
            xATRTrailingStop[i+1]=df['HA_Close'][i+1] - nLoss[i+1]
        else:
            xATRTrailingStop[i+1]=df['HA_Close'][i+1] + nLoss[i+1]

        #if(df['HA_Close'][i+1]>xATRTrailingStop[i+1] and df['HA_Close'][i]<=xATRTrailingStop[i]):
            #buy=True
            #df['utbot_signal_'+str(atr_period)][i+1] =  "BUY"
            
        if(df['HA_Close'][i+1]<xATRTrailingStop[i+1] and df['HA_Close'][i]>=xATRTrailingStop[i]):
            sell=True
            df['utbot_signal_SELL'][i+1] =  "SELL"
        else :
            df['utbot_signal_SELL'][i+1] = "" 
    
    df['xATRTrailingStop'] = xATRTrailingStop
       
    
    '''
    if(df['HA_Close'][last_key]>xATRTrailingStop[last_key] and df['HA_Close'][previous_key]<=xATRTrailingStop[previous_key]):
        buy=True
        df['utbot_signal_',str(atr_period)][last_key] =  "Buy"
    elif(df['HA_Close'][last_key]<xATRTrailingStop[last_key] and df['HA_Close'][previous_key]>=xATRTrailingStop[previous_key]):
        sell=True
        df['utbot_signal_',str(atr_period)][last_key] =  "Sell"
    '''
    
    return df



def UTBot_Buy(df,sensitivity,atr_period,ha_signals):
    
    pd.set_option('display.max_rows', None)
    
    last_key=df['HA_Close'].keys()[-1]
    previous_key=df['HA_Close'].keys()[-2]
    previous_key_2=df['HA_Close'].keys()[-3]
    
    
    df=calculate_period_atr(df,atr_period)
    xATR=df['ATR']
    
    nLoss = pd.Series(len(df['HA_Close']))                              
    above = pd.Series(len(df['HA_Close']))
    below = pd.Series(len(df['HA_Close']))
    
    nLoss = sensitivity * xATR
    
    if (ha_signals):
        df = getHA(df)
   
    xATRTrailingStop = pd.Series(len(df['HA_Close']))
    
    for i in range(len(df['HA_Close'])-1) :                                 
        xATRTrailingStop[i]=0.0
        above[i]=False
        below[i]=False
        
    df['utbot_signal_BUY'] = "" 
    calculate_ema_ha(df,1)
    ema = df['ema1']
    
    for i in range(len(df['HA_Close'])-1):

        buy = False
        sell = False    
        
        if(df['HA_Close'][i+1] > nz(xATRTrailingStop[i],0) and df['HA_Close'][i] > nz(xATRTrailingStop[i],0)):
            xATRTrailingStop[i+1]=max(nz(xATRTrailingStop[i]),df['HA_Close'][i+1] - nLoss[i+1])
        elif (df['HA_Close'][i+1] < nz(xATRTrailingStop[i],0) and df['HA_Close'][i] < nz(xATRTrailingStop[i],0)):
            xATRTrailingStop[i+1]=min(nz(xATRTrailingStop[i]),df['HA_Close'][i+1] + nLoss[i+1])
        elif(df['HA_Close'][i+1] > nz(xATRTrailingStop[i],0)):
            xATRTrailingStop[i+1]=df['HA_Close'][i+1] - nLoss[i+1]
        else:
            xATRTrailingStop[i+1]=df['HA_Close'][i+1] + nLoss[i+1]
                
        if (ema[i]<xATRTrailingStop[i] and ema[i+1]>xATRTrailingStop[i+1]) :
            above[i] = True
        if (ema[i]>xATRTrailingStop[i] and ema[i+1]<xATRTrailingStop[i+1]) :
            below[i] = True
    
        if(above[i] and 
           (df['HA_Close'][i+1]>xATRTrailingStop[i+1])):
            buy=True
            df['utbot_signal_BUY'][i+1] =  "BUY"
        #elif(below[i] and 
             #(df['HA_Close'][i+1]<xATRTrailingStop[i+1])):
            #sell=True
            #df['utbot_signal_SELL'][i+1] =  "SELL"
        else :
            df['utbot_signal_BUY'][i+1] = ""  
    
    df['xATRTrailingStop'] = xATRTrailingStop
    
    return df




def UTBot_Sell(df,sensitivity,atr_period,ha_signals):

    pd.set_option('display.max_rows', None)
    
    last_key=df['HA_Close'].keys()[-1]
    previous_key=df['HA_Close'].keys()[-2]
    previous_key_2=df['HA_Close'].keys()[-3]
    
    
    df=calculate_period_atr(df,atr_period)
    xATR=df['ATR']
    
    nLoss = pd.Series(len(df['HA_Close']))                              
    above = pd.Series(len(df['HA_Close']))
    below = pd.Series(len(df['HA_Close']))
    
    nLoss = sensitivity * xATR
    
    if (ha_signals):
        df = getHA(df)
   
    xATRTrailingStop = pd.Series(len(df['HA_Close']))
    
    for i in range(len(df['HA_Close'])-1) :   
        xATRTrailingStop[i]=0.0
        above[i]=False
        below[i]=False
        
    df['utbot_signal_SELL'] = "" 
    calculate_ema_ha(df,1)
    ema = df['ema1']
    
    for i in range(len(df['HA_Close'])):

        buy = False
        sell = False    
        if (i>0):
            if(df['HA_Close'][i] > nz(xATRTrailingStop[i-1],0) and df['HA_Close'][i-1] > nz(xATRTrailingStop[i-1],0)):
                xATRTrailingStop[i]=max(nz(xATRTrailingStop[i-1]),df['HA_Close'][i] - nLoss[i])
            elif (df['HA_Close'][i] < nz(xATRTrailingStop[i-1],0) and df['HA_Close'][i-1] < nz(xATRTrailingStop[i-1],0)):
                xATRTrailingStop[i]=min(nz(xATRTrailingStop[i-1]),df['HA_Close'][i] + nLoss[i])
            elif(df['HA_Close'][i] > nz(xATRTrailingStop[i-1],0)):
                xATRTrailingStop[i]=df['HA_Close'][i] - nLoss[i]
            else:
                xATRTrailingStop[i]=df['HA_Close'][i] + nLoss[i]

            if (ema[i-1]<xATRTrailingStop[i-1] and ema[i]>xATRTrailingStop[i]) :
                above[i-1] = True
            if (ema[i-1]>xATRTrailingStop[i-1] and ema[i]<xATRTrailingStop[i]) :
                below[i-1] = True

            #if(above[i-1] and 
               #(df['HA_Close'][i]>xATRTrailingStop[i])):
                #buy=True
                #df['utbot_signal_BUY'][i] =  "BUY"
            if(below[i-1] and 
               (df['HA_Close'][i]<xATRTrailingStop[i])):
                sell=True
                df['utbot_signal_SELL'][i] =  "SELL"
            else :
                df['utbot_signal_SELL'][i] = ""  
    
    df['xATRTrailingStop'] = xATRTrailingStop
    
    return df


def stc(Data, st_ema, lt_ema, stoch_lookback, what, where):
    
    Data = ema(Data, 2, st_ema, what, where)
    Data = ema(Data, 2, lt_ema, what, where + 1)
    
    # MACD Line
    Data[:, where + 2] = Data[:, where] - Data[:, where + 1]    
    # %K
    for i in range(len(Data)):
            
            try:
                Data[i, where + 3] = 100 * (Data[i, where + 2] - min(Data[i - stoch_lookback + 1:i + 1, where + 2])) / (max(Data[i - stoch_lookback + 1:i + 1, where + 2]) - min(Data[i - stoch_lookback + 1:i + 1, where + 2]))
            
            except ValueError:
                pass
    
    # %D        
    Data = ma(Data, 3, where + 3, where + 4)
    
    return Data


def nz(x, y=None):
    '''
    RETURNS
    Two args version: returns x if it's a valid (not NaN) number, otherwise y
    One arg version: returns x if it's a valid (not NaN) number, otherwise 0
    ARGUMENTS
    x (val) Series of values to process.
    y (float) Value that will be inserted instead of all NaN values in x series.
    
    if isinstance(x, np.generic):
        return x.fillna(y or 0)
    if x != x:
        if y is not None:
            return y
        return 0

    '''
    
    if not math.isnan(x) :
        return x
    else:
        if y is not None:
            return y
        return 0
    
    
    
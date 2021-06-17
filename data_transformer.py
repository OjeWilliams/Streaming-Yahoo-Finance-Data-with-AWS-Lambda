# All dependencies including the yfinance module were added as a lambda layer

# Importing required packages
import json
import boto3
import random
import time
import datetime

import yfinance as yf 

from time import sleep

def lambda_handler(event, context):
    
    # start timer
    start = time.time()
    
    # create the boto3 client and name it kinesis
    kinesis = boto3.client('kinesis', "us-east-2")
    
    # set the required start and end dates
    begin = '2021-05-11'
    stop  = '2021-05-12'
    
    # setting the required tickers and the fetch interval 
    # tickers = "FB SHOP BYND NFLX PINS SQ TTD OKTA SNAP DDOG"
    tickers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
    interval = "5m"
    
    
    rowcount = 0 # intialize variable to count records
    
    # loops through each stock and downloads their data
    for tick in tickers:
        stock = yf.Ticker(tick)
        data = stock.history(start=begin, end=stop, interval = interval)
        
        # loops over each of the data rows received
        total_data ={} # dictionary to store data
        for index, row in data.iterrows():
            
            # creates a dictionary to store in required format
            kinesis_record = {
            "high":row["High"],
            "low":row["Low"], 
            "ts":index.strftime('%Y-%m-%d %H:%M:%S'), 
            "name":tick
            }
            
            # converts the dictionary to JSON
            total_data = json.dumps(kinesis_record)+"\n"
            
            print(total_data) # take a look at the single record
            
            # sends a single record to the Kinesis Stream
            kinesis.put_record(
                    StreamName="STA9760SP21_stream1",
                    Data=total_data.encode('utf-8'),
                    PartitionKey="partitionkey")
            sleep(0.5)
            
            # increment our rowcount
            rowcount = rowcount + 1
    
    #end timer
    end = time.time()
    
    # total time and total records
    print("The total runtime: " + str(end - start))        
    print(f"The total number of records sent to Kinesis: {rowcount}")       
    
    return {
        'statusCode': 200,
        'body': json.dumps("Run Complete!")
    }      
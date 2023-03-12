import requests
import asyncio
import json
import pandas as pd
import requests, sys

BASE_URL = 'https://nevtestapp.azurewebsites.net/api/SendEventsToEventHub?'

def internet_resource_getter(post_data):
    stuff_got = []

    response = requests.post(BASE_URL, data=post_data)
    stuff_got.append(response)
    
    print(response)
    return stuff_got

loop = asyncio.get_event_loop()
file_name = sys.argv[1]
    # Read the file
data = pd.read_csv(f'{file_name}', sep='|')

for i in data.index:
    # print(i)
    try:
        # convert the row to json
        export = data.loc[i].to_json()
        # send it to the api
        export = export.replace("'","")
        # print(export)
        # export = export.replace("\"","")
        # response = requests.post(myURL, data=export)
        # with ThreadPool(10) as pool: #ten requests to run in paralel
        #     output_list = list(pool.map(get_url, export))

        response = loop.run_in_executor(None, internet_resource_getter, export)

        # print the returncode
        # print(response)
    except:
        print(data.loc[i])
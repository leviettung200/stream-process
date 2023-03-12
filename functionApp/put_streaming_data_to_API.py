import pandas as pd
import requests, sys

# write all the rows from local .csv file to the api as Put request
def myfunction():
    myURL = "https://nevtestapp.azurewebsites.net/api/SendEventsToEventHub?"
    # myURL = "https://kltnapp.azurewebsites.net/api/SendEventsToEventHub"
    # myURL = "http://localhost:7071/api/SendEventsToEventHub"
    # Read the file
    data = pd.read_csv(f'../data/{sys.argv[1]}', sep='|')

    for i in data.index:
        # print(i)
        try:
            # convert the row to json
            export = data.loc[i].to_json()
            export = export.replace("'", "")
            # send it to the api
            response = requests.post(myURL, data=export)
            # print(type(export))
            # print the returncode
            # print(export)
            print(response)
        except:
            print(data.loc[i])

if __name__ == "__main__":
    myfunction()
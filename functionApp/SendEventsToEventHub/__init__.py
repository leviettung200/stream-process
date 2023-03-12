import logging
import jsonschema
from jsonschema import validate
import azure.functions as func
import time
import asyncio
import os

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

CONNECTION_STR = os.environ["AzureEventHubConnectionString"]
EVENTHUB_NAME = os.environ["AzureEventHubName"]


async def run(data):

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )

    async with producer:
        event_data_batch = await producer.create_batch()
        # for i in range(1):
        event_data = EventData(data)
        try:
            event_data_batch.add(event_data)
        except ValueError:
            await producer.send_batch(event_data_batch)
            event_data_batch = await producer.create_batch()
            event_data_batch.add(event_data)
        if len(event_data_batch) > 0:
            await producer.send_batch(event_data_batch)
        


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_json = req.get_json()
    req_body = req.get_body()

    trans_schema = {
  "type": "object",
  "properties": {
    "ssn": {
      "type": "string"
    },
    "cc_num": {
      "type": "integer"
    },
    "first": {
      "type": "string"
    },
    "last": {
      "type": "string"
    },
    "gender": {
      "type": "string"
    },
    "street": {
      "type": "string"
    },
    "city": {
      "type": "string"
    },
    "state": {
      "type": "string"
    },
    "zip": {
      "type": "integer"
    },
    "lat": {
      "type": "number"
    },
    "long": {
      "type": "number"
    },
    "city_pop": {
      "type": "integer"
    },
    "job": {
      "type": "string"
    },
    "dob": {
      "type": "string"
    },
    "acct_num": {
      "type": "integer"
    },
    "profile": {
      "type": "string"
    },
    "trans_num": {
      "type": "string"
    },
    "trans_date": {
      "type": "string"
    },
    "trans_time": {
      "type": "string"
    },
    "unix_time": {
      "type": "integer"
    },
    "category": {
      "type": "string"
    },
    "amt": {
      "type": "number"
    },
    "is_fraud": {
      "type": "integer"
    },
    "merchant": {
      "type": "string"
    },
    "merch_lat": {
      "type": "number"
    },
    "merch_long": {
      "type": "number"
    }
  }
#   ,
#   "required": [
#     "ssn",
#     "cc_num",
#     "first",
#     "last",
#     "gender",
#     "street",
#     "city",
#     "state",
#     "zip",
#     "lat",
#     "long",
#     "city_pop",
#     "job",
#     "dob",
#     "acct_num",
#     "profile",
#     "trans_num",
#     "trans_date",
#     "trans_time",
#     "unix_time",
#     "category",
#     "amt",
#     "is_fraud",
#     "merchant",
#     "merch_lat",
#     "merch_long"
#   ]
}
    # method = req_body['context']['http-method']
    # method = event['http-method']
    # if method == "POST":
    if 1:
        # p_record = event['body-json']
        # print (p_record['id'])
        # print (p_record)
        
        def validateJson(jsonData):
            try:
                # Validate will raise exception if given json is not
                validate(instance=jsonData, schema=trans_schema)
            except jsonschema.exceptions.ValidationError as err:
                return False
            return True
        
        isValid = validateJson(req_json)
        
        if isValid:
            # print('p_record',p_record)
            # print("Given JSON data is Valid")
            # recordstring = json.dumps(p_record)
            # print('recordstring',recordstring)
                # Convert dict to string
            # req_body = json.dumps(req_body).encode()

            start_time = time.time()
            asyncio.run(run(req_body))
            logging.info("Send messages in {} seconds.".format(time.time() - start_time))
            
        else:
            # print('p_record',p_record)
            #validate(instance=p_record, schema=trans_schema)
            print("Given JSON data is InValid")
            logging.info(req_json)
            # generate the name for the file with the timestamp to write to s3
            # mykey = 'output-' + timestampStr + str(round(time.time() * 1000)) + '.txt'
                        
            #put the file into the s3 bucket
            # response = s3_client.put_object(Body=json.dumps(p_record), Bucket='00-s3-invalid-transactions', Key= mykey)
    return func.HttpResponse("", status_code=200)



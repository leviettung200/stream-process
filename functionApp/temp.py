import logging, json, pyodbc
from typing import List

import azure.functions as func


def main(events: List[func.EventHubEvent]):
    host = "kltn.database.windows.net"
    database = "card_transaction"
    username = "tunglv"
    password = "Tung13032000"
    driver= '{ODBC Driver 17 for SQL Server}'

    # try:
    
    # except:
    #     logging.info("Connect failed to SQL DB")
    #     pass
    for event in events:
        # logging.info('Python EventHub trigger processed an event: %s',
        #          event.get_body().decode('utf-8'))
        conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+host+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    
        cursor = conn.cursor()

        event_body = event.get_body().decode('utf-8')
        event_json = json.loads(event_body)
        # logging.info(type(event_json))

        sql = f"""
        MERGE CITIES WITH (SERIALIZABLE) AS T 
        USING (VALUES ('{event_json['city']}', '{event_json['state']}','{event_json['city_pop']}')) AS U (CITY_NAME, STATE, CITY_POPULATION) ON U.CITY_NAME = T.CITY_NAME and U.STATE = T.STATE 
        WHEN MATCHED THEN 
            UPDATE SET T.CITY_NAME = U.CITY_NAME, T.STATE = U.STATE, T.CITY_POPULATION = U.CITY_POPULATION 
        WHEN NOT MATCHED THEN 
            INSERT (CITY_NAME, STATE, CITY_POPULATION) VALUES (U.CITY_NAME, U.STATE, U.CITY_POPULATION);"""
        logging.info(sql)
        response = cursor.execute(sql)

        sql = f"""
        MERGE ADDRESSES WITH (SERIALIZABLE) AS T 
        USING (VALUES ('{event_json['street']}', '{event_json['zip']}','{event_json['lat']}','{event_json['long']}')) AS U (STREET, ZIP, LAT, LONGI) ON U.STREET = T.STREET and U.ZIP = T.ZIP 
        WHEN MATCHED THEN 
            UPDATE SET T.STREET = U.STREET, T.ZIP = U.ZIP, T.LAT = U.LAT,  T.LONGI = U.LONGI
        WHEN NOT MATCHED THEN 
            INSERT (STREET, ZIP, LAT, LONGI) VALUES (U.STREET, U.ZIP, U.LAT, U.LONGI);"""
        response = cursor.execute(sql)

        sql =f"""update  a
            set a.CITY_ID = c.CITY_ID
            from ADDRESSES a inner join CITIES c 
            on a.CITY_ID = c.CITY_ID
            where a.STREET = '{event_json['street']}' and a.ZIP = '{str(event_json['zip'])}'and c.CITY_NAME='{event_json['city']}' and c.STATE='{event_json['state']}'

        """
        response = cursor.execute(sql)

        # #------------insert/update into 'CUSTOMERS' table 

        sql = f"""
        MERGE CUSTOMERS WITH (SERIALIZABLE) AS T 
        USING (VALUES ('{event_json['first']}', '{event_json['last']}',{event_json['cc_num']},'{event_json['gender']}','{event_json['job']}','{event_json['dob']}')) AS U (FIRST_NAME, LAST_NAME, CREDIT_CARD_NUMBER, GENDER, JOB, DATE_OF_BIRTH) ON U.CREDIT_CARD_NUMBER = T.CREDIT_CARD_NUMBER 
        WHEN MATCHED THEN 
            UPDATE SET T.FIRST_NAME = U.FIRST_NAME, T.LAST_NAME = U.LAST_NAME, T.CREDIT_CARD_NUMBER = U.CREDIT_CARD_NUMBER,  T.GENDER = U.GENDER, T.JOB = U.JOB, T.DATE_OF_BIRTH = U.DATE_OF_BIRTH
        WHEN NOT MATCHED THEN 
            INSERT (FIRST_NAME, LAST_NAME, CREDIT_CARD_NUMBER, GENDER, JOB, DATE_OF_BIRTH) VALUES (U.FIRST_NAME, U.LAST_NAME, U.CREDIT_CARD_NUMBER, U.GENDER, U.JOB, U.DATE_OF_BIRTH);"""
        response = cursor.execute(sql)

        # sql ="update CUSTOMERS c inner join ADDRESSES a set c.ADDR_ID = a.ADDR_ID where c.CREDIT_CARD_NUMBER='" + str(event_json['cc_num']) + "' and a.STREET='" + event_json['street'] + "' and a.ZIP=" + str(event_json['zip']) + ""
        sql =f"""update  c
            set c.ADDR_ID = a.ADDR_ID
            from CUSTOMERS c inner join ADDRESSES a 
            on c.ADDR_ID = a.ADDR_ID
            where c.CREDIT_CARD_NUMBER = {event_json['cc_num']} and a.STREET = '{event_json['street']}'and a.ZIP='{str(event_json['zip'])}'
        """
        response = cursor.execute(sql)

        # #------------insert into 'MERCHANTS' table 
            
        sql = f"""
        MERGE MERCHANTS WITH (SERIALIZABLE) AS T 
        USING (VALUES ('{event_json['merchant']}', '{event_json['category']}','{event_json['merch_lat']}','{event_json['merch_long']}')) AS U (MERCHANT_NAME, CATEGORY, LAT, LONGI) ON U.MERCHANT_NAME = T.MERCHANT_NAME and U.CATEGORY = T.CATEGORY 
        WHEN MATCHED THEN 
            UPDATE SET T.MERCHANT_NAME = U.MERCHANT_NAME, T.CATEGORY = U.CATEGORY, T.LAT = U.LAT,  T.LONGI = U.LONGI
        WHEN NOT MATCHED THEN 
            INSERT (MERCHANT_NAME, CATEGORY, LAT, LONGI) VALUES (U.MERCHANT_NAME, U.CATEGORY, U.LAT, U.LONGI);"""
        response = cursor.execute(sql)

        #------------insert/update into 'TRANSACTIONS' table 
            
        # sql ="insert into TRANSACTIONS(TRANSACTION_ID, AMOUNT, TRANSFER_TIMESTAMP, UNIX_TIME, IS_FRAUD) values ('" + event_json['trans_num'] + "','" + str(event_json['amt']) + "','" + str(event_json['trans_date_trans_time']) + "'," + str(event_json['unix_time']) + " , '" + str(event_json['is_fraud']) + "') ON DUPLICATE KEY UPDATE TRANSACTION_ID = VALUES(TRANSACTION_ID), AMOUNT = VALUES(AMOUNT), TRANSFER_TIMESTAMP = VALUES(TRANSFER_TIMESTAMP) , UNIX_TIME = VALUES(UNIX_TIME) , IS_FRAUD = VALUES(IS_FRAUD)"
        sql = f"""
        MERGE TRANSACTIONS WITH (SERIALIZABLE) AS T 
        USING (VALUES ('{event_json['trans_num']}', '{str(event_json['amt'])}','{str(event_json['trans_date_trans_time'])}',{str(event_json['unix_time'])},'{str(event_json['is_fraud'])}')) AS U (TRANSACTION_ID, AMOUNT, TRANSFER_TIMESTAMP, UNIX_TIME, IS_FRAUD) ON T.TRANSACTION_ID = U.TRANSACTION_ID 
        WHEN MATCHED THEN 
            UPDATE SET T.TRANSACTION_ID = U.TRANSACTION_ID, T.AMOUNT = U.AMOUNT, T.TRANSFER_TIMESTAMP = U.TRANSFER_TIMESTAMP,  T.UNIX_TIME = U.UNIX_TIME,  T.IS_FRAUD = U.IS_FRAUD
        WHEN NOT MATCHED THEN 
            INSERT (TRANSACTION_ID, AMOUNT, TRANSFER_TIMESTAMP, UNIX_TIME, IS_FRAUD) VALUES (U.TRANSACTION_ID, U.AMOUNT, U.TRANSFER_TIMESTAMP, U.UNIX_TIME, U.IS_FRAUD);"""
        response = cursor.execute(sql)
        

        # sql ="update TRANSACTIONS t inner join CUSTOMERS c set t.CUST_ID = c.CUST_ID where t.TRANSACTION_ID='" + event_json['trans_num']+ "' and c.CREDIT_CARD_NUMBER='" + str(event_json['cc_num']) + "'"
        sql =f"""update  t
            set t.CUST_ID = c.CUST_ID
            from  TRANSACTIONS t inner join CUSTOMERS c
            on t.CUST_ID = c.CUST_ID
            where t.TRANSACTION_ID = '{event_json['trans_num']}' and c.CREDIT_CARD_NUMBER = {event_json['cc_num']}
        """
        response = cursor.execute(sql)
    
        # sql ="update TRANSACTIONS t inner join MERCHANTS m set t.MERCHANT_ID = m.MERCHANT_ID where t.TRANSACTION_ID='" + event_json['trans_num']+ "' and m.MERCHANT_NAME='" + event_json['merchant']+ "' and m.CATEGORY='" + event_json['category']+ "'"
        sql =f"""update  t
            set t.MERCHANT_ID = m.MERCHANT_ID
            from  TRANSACTIONS t inner join MERCHANTS m
            on t.MERCHANT_ID = m.MERCHANT_ID
            where t.TRANSACTION_ID = '{event_json['trans_num']}' and m.MERCHANT_NAME = '{event_json['merchant']}' and m.CATEGORY='{event_json['category']}'
        """
        response = cursor.execute(sql)
        # cursor.execute("COMMIT TRAN")
            

        # try:

        #     #------------insert into 'CITIES' table 
        #     sql ="insert into CITIES(CITY_NAME, STATE, CITY_POPULATION) values ('" + event_json['city'] + "','" + event_json['state'] + "', '" + event_json['city_pop'] + "' ) ON DUPLICATE KEY UPDATE CITY_NAME = VALUES(CITY_NAME), STATE = VALUES(STATE), CITY_POPULATION = VALUES(CITY_POPULATION)"
        #     response = cursor.execute(sql)
            # print(transaction['transactionId'])
            
        
            #------------insert/update into 'ADDRESSES' table 
            # sql ="insert into ADDRESSES(STREET, ZIP, LAT, LONGI) values ('" + event_json['street'] + "','" + str(event_json['zip']) + "', '" + str(event_json['lat']) + "' , '" + str(event_json['long']) + "') ON DUPLICATE KEY UPDATE STREET = VALUES(STREET), ZIP = VALUES(ZIP), LAT = VALUES(LAT) , LONGI = VALUES(LONGI)"
            # response = cursor.execute(sql)
            
        
            # sql ="update ADDRESSES a inner join CITIES c set a.CITY_ID = c.CITY_ID where a.STREET='" + event_json['street'] + "' and a.ZIP=" + str(event_json['zip']) + " and c.CITY_NAME='"+ event_json['city'] + "' and c.STATE='"+ event_json['state'] + "'"
            # response = cursor.execute(sql)
            
            # #------------insert/update into 'CUSTOMERS' table 

            # sql ="insert into CUSTOMERS(FIRST_NAME, LAST_NAME, CREDIT_CARD_NUMBER, GENDER, JOB, DATE_OF_BIRTH) values ('" + event_json['first'] + "','" + event_json['last'] + "', '" + str(event_json['cc_num']) + "' , '" + event_json['gender'] + "', '" + event_json['job'] + "', '" + event_json['dob'] + "') ON DUPLICATE KEY UPDATE FIRST_NAME = VALUES(FIRST_NAME), LAST_NAME = VALUES(LAST_NAME), CREDIT_CARD_NUMBER = VALUES(CREDIT_CARD_NUMBER) , GENDER = VALUES(GENDER) , JOB = VALUES(JOB), DATE_OF_BIRTH = VALUES(DATE_OF_BIRTH)"
            # response = cursor.execute(sql)
            
            
            # sql ="update CUSTOMERS c inner join ADDRESSES a set c.ADDR_ID = a.ADDR_ID where c.CREDIT_CARD_NUMBER='" + str(event_json['cc_num']) + "' and a.STREET='" + event_json['street'] + "' and a.ZIP=" + str(event_json['zip']) + ""
            # response = cursor.execute(sql)
           
            # #------------insert into 'MERCHANTS' table 
            
            # sql ="insert into MERCHANTS(MERCHANT_NAME, CATEGORY, LAT, LONGI) values ('" + event_json['merchant'] + "','" + event_json['category'] + "', '" + str(event_json['merch_lat']) + "' , '" + str(event_json['merch_long']) + "') ON DUPLICATE KEY UPDATE MERCHANT_NAME = VALUES(MERCHANT_NAME), CATEGORY = VALUES(CATEGORY), LAT = VALUES(LAT) , LONGI = VALUES(LONGI)"
            # response = cursor.execute(sql)
        
            # #------------insert/update into 'TRANSACTIONS' table 
            
            # sql ="insert into TRANSACTIONS(TRANSACTION_ID, AMOUNT, TRANSFER_TIMESTAMP, UNIX_TIME, IS_FRAUD) values ('" + event_json['trans_num'] + "','" + str(event_json['amt']) + "','" + str(event_json['trans_date_trans_time']) + "', '" + str(event_json['unix_time']) + "' , '" + str(event_json['is_fraud']) + "') ON DUPLICATE KEY UPDATE TRANSACTION_ID = VALUES(TRANSACTION_ID), AMOUNT = VALUES(AMOUNT), TRANSFER_TIMESTAMP = VALUES(TRANSFER_TIMESTAMP) , UNIX_TIME = VALUES(UNIX_TIME) , IS_FRAUD = VALUES(IS_FRAUD)"
            # response = cursor.execute(sql)
            

            # sql ="update TRANSACTIONS t inner join CUSTOMERS c set t.CUST_ID = c.CUST_ID where t.TRANSACTION_ID='" + event_json['trans_num']+ "' and c.CREDIT_CARD_NUMBER='" + str(event_json['cc_num']) + "'"
            # response = cursor.execute(sql)
        
            # sql ="update TRANSACTIONS t inner join MERCHANTS m set t.MERCHANT_ID = m.MERCHANT_ID where t.TRANSACTION_ID='" + event_json['trans_num']+ "' and m.MERCHANT_NAME='" + event_json['merchant']+ "' and m.CATEGORY='" + event_json['category']+ "'"
            # response = cursor.execute(sql)
            # cursor.execute("COMMIT TRAN")
            
        # except Exception as e:
        #     print(f'Error: {e}')
            # transaction_response = client.rollback_transaction(
            #     secretArn=db_credentials_secrets_store_arn,
            #     resourceArn=db_cluster_arn,
            #     transactionId=transaction['transactionId'])
                
        # else:
            # transaction_response = client.commit_transaction(
            #     secretArn=db_credentials_secrets_store_arn,
            #     resourceArn=db_cluster_arn,
            #     transactionId=transaction['transactionId'])
            # print(transaction['transactionId'])
            
            # print("end transaction")
        conn.commit()
        # print(f'Transaction Status: {transaction_response["transactionStatus"]}')
        print("end")
        
    



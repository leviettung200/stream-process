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

        # --dim address entry
        query = f"""insert into dbo.dim_address (street, zip, lat, long, city, state,  city_pop)
        values ('{event_json['street']}',{event_json['zip']},{event_json['lat']}, {event_json['long']}, '{event_json['city']}', '{event_json['state']}',{event_json['city_pop']})
        """
        logging.info(query)
        response = cursor.execute(query)
        # --dim customer entry
        # insert into dev.public.dim_customer (first, last, cc_num, gender, job, dob)
        # select first, last, cc_num, gender, job, dob from dev.public.temp_stage where nrum = act_row_flag and cc_num not in (select cc_num from dev.public.dim_customer);


        # --dim merchant entry
        # insert into dev.public.dim_merchant (merchant, category, merch_lat, merch_long) 
        # select merchant, category, merch_lat, merch_long from dev.public.temp_stage where nrum = act_row_flag and concat(merchant, category) not in (select concat(merchant, category) from dev.public.dim_merchant);


        # --dim time entry
        # insert into dev.public.dim_time ( hour, day, month, quarter, year )
        # select --cast(trans_date_trans_time as timestamp), 
        # cast(to_char(cast(trans_date_trans_time as timestamp), 'HH24') as int), 
        # cast(to_char(cast(trans_date_trans_time as timestamp), 'dd') as int), to_char(cast(trans_date_trans_time as timestamp), 'MON'), 
        #     case when (to_char(cast(trans_date_trans_time as timestamp), 'MON')) in ('APR','MAY','JUN')  then 'Q2' 
        #         when (to_char(cast(trans_date_trans_time as timestamp), 'MON')) in ('JAN','FEB','MAR')  then 'Q1'     
        #         when (to_char(cast(trans_date_trans_time as timestamp), 'MON')) in ('JUL','AUG','SEP')  then 'Q3'  
        #         when (to_char(cast(trans_date_trans_time as timestamp), 'MON')) in ('OCT','NOV','DEC')  then 'Q4'  
        #                         end,
        # cast(to_char(cast(trans_date_trans_time as timestamp), 'YYYY') as int)
        # from dev.public.temp_stage where nrum = act_row_flag and to_char(cast(trans_date_trans_time as timestamp), 'HH24')|| to_char(cast(trans_date_trans_time as timestamp), 'dd')|| to_char(cast(trans_date_trans_time as timestamp), 'MON') not in (select  right(0 ||  hour,2) || right(0 || day,2)  || month from  dev.public.dim_time );

                                
        # -- fact entry 
        #     insert into dev.public.fact_transaction 
            
        #     select c.cust_id, a.addr_id, m.merchant_id, t.time_id, tt.amt, tt.is_fraud
        #     from (   select cust_id, 'xx' as join_cl from dev.public.dim_customer where cc_num in (select cc_num from dev.public.temp_stage where nrum = act_row_flag)    ) c
        #     join (   select addr_id, 'xx' as join_cl from dev.public.dim_address where concat(street, zip) in (select concat(street, zip) from dev.public.temp_stage where nrum = act_row_flag) ) a
        #     on  c.join_cl = a.join_cl
        #     join (   select merchant_id, 'xx' as join_cl from dev.public.dim_merchant where concat(merchant, category) in (select concat(merchant, category) from dev.public.temp_stage where nrum = act_row_flag) )    m
        #     on  c.join_cl = m.join_cl 
        #     join (   select time_id, 'xx' as join_cl  from dev.public.dim_time where (hour || day || month || year) in (select cast(to_char(cast(trans_date_trans_time as timestamp), 'HH24') as int) ||  cast(to_char(cast(trans_date_trans_time as timestamp), 'dd') as int) ||  to_char(cast(trans_date_trans_time as timestamp), 'MON') || cast(to_char(cast(trans_date_trans_time as timestamp), 'YYYY') as int)  from dev.public.temp_stage  where nrum = act_row_flag)    ) t
        # on  c.join_cl = t.join_cl 
        # join (select amt, is_fraud, 'xx' as join_cl from dev.public.temp_stage where nrum = act_row_flag  ) tt
        # on c.join_cl = tt.join_cl ;

        conn.commit()
        # print(f'Transaction Status: {transaction_response["transactionStatus"]}')
        print("end")
        
    



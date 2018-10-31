
#------This code is written to extract the BigQuery resultant table for building recommendations----
# Before running this code, authentication with google api has to be done
# export GOOGLE_APPLICATION_CREDENTIALS=~/Documents/SearchTeam/kouzoh-p-codechaitu-16cf63a60a1c.json
import pandas as pd
from google.cloud import bigquery
import sys

csvFileName = 'user_item_price.csv'
client = bigquery.Client()
query_job = client.query(''' SELECT * FROM `kouzoh-p-codechaitu.RECSYS.item_user_price` LIMIT 2000''') # change to required query for execution
fetchResults = query_job.result()
df = fetchResults.to_dataframe()
df.to_csv(csvFileName,index=False)


#--- Actual query run on sage-shared-720, after executing, copied the results to my cloud---
'''
    SELECT buyer_id as user_id, T.item_id, price FROM `sage-shard-740.anon_jp.transaction_evidences`   as T   INNER JOIN (
    SELECT user_id, JSON_EXTRACT_SCALAR(prop, '$.item_id') as item_id  FROM `sage-shard-740.pascal_event_log.event_log_201810*`WHERE  event_id = 'item_detail_display' AND _TABLE_SUFFIX BETWEEN '25' and '30'  AND user_id IS NOT NULL )   as L     ON T.buyer_id = L.user_id AND T.item_id = L.item_id WHERE T.status='wait_payment'        LIMIT 2000
'''
    
    
    

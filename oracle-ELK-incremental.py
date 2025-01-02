import cx_Oracle
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import json
from datetime import datetime, timezone
import pytz
import sys
import traceback
from customer360_parameters import *

db_credential = {
    "host_address" : olap_prod_host_address,
    "port" : olap_prod_port,
    "service_name" : olap_prod_service_name,
    "user_name" : olap_prod_user_name,
    "password" : olap_prod_password
}

es_credential = {
    "host_address": elk_host_address,
    "user_name": elk_user_name,
    "password": elk_password
}

sql_file_path = 'Query.txt'
with open(sql_file_path, 'r') as file:
    query = file.read()
    
#Database Connection Establish
def database_connection(db_credential):
    try:
        print("DATABASE CONNECTING..............")
        dsn_tns = cx_Oracle.makedsn(db_credential['host_address'], db_credential['port'], service_name=db_credential['service_name'])
        conn = cx_Oracle.connect(user=db_credential['user_name'], password=db_credential['password'], dsn=dsn_tns)
        return conn
    except Exception as err:
        print("DATABASE CONNECTION ERROR ",err) 
        traceback.print_exc()
        raise SystemExit

def get_db_data(query, conn):
    try:
        print("FETCHING DATA FROM DB...............")
        df_query_join=pd.read_sql_query(query, conn)
        return df_query_join
    except Exception as err:
        print("NEW INVOICE DATABASE ERROR",err)    
        traceback.print_exc()
        raise SystemExit

def data_conversion(new_data_frame):
    try:
        print("DATA CONVERSION..........")
        #creating Data frame
        result = pd.DataFrame(new_data_frame)
        result_json_data = result.to_json(orient='records')
        result = json.loads(result_json_data)
        return result
    except Exception as err:
        print("DATA CONVERSION ERROR",err)
        traceback.print_exc()
        raise SystemExit
    
def epoch_to_dt(epoch_time):
    return datetime.fromtimestamp((epoch_time / 1000),tz=timezone.utc)

def transform_new_data(index_name,result):
    try:
        print("DATA TRANSFORMATION..............")
        details_data  = []
        for data in result:
            dict_data = {}
            dict_data['ACCNT_ARN'] = data['ACCNT_ARN']
            dict_data['ACCNT_NAME'] = data['ACCNT_NAME']
            dict_data['ACCNT_STATUS'] = data['ACCNT_STATUS']
            dict_data['ACCNT_TYPE_CD'] = data['ACCNT_TYPE_CD']
            # dict_data['ASSESS_ATTRIB_VAL'] = data['ASSESS_ATTRIB_VAL']

            if (data['ASSESS_DT'] is not None) and (str(data['ASSESS_DT']).isdigit()):
                dict_data['ASSESS_DT'] = epoch_to_dt(data['ASSESS_DT']).strftime('%d-%b-%Y')

            dict_data['ASSESS_NAME'] = data['ASSESS_NAME']
            dict_data['ASSESS_SCORE'] = data['ASSESS_SCORE']
            dict_data['ASSESS_STATUS'] = data['ASSESS_STATUS']
            dict_data['ASSET_LOB'] = data['ASSET_LOB']
            dict_data['ASSET_NUM'] = data['ASSET_NUM']
            dict_data['ASSET_PL'] = data['ASSET_PL']
            dict_data['ASSET_PPL'] = data['ASSET_PPL']
            dict_data['ASSET_VC'] = data['ASSET_VC']
            dict_data['CON_CONTACT_ID'] = data['CON_CONTACT_ID']
            dict_data['CON_CRN_NO'] = data['CON_CRN_NO']
            dict_data['CON_FST_NAME'] = data['CON_FST_NAME']
            dict_data['CON_LAST_NAME'] = data['CON_LAST_NAME']
            dict_data['CON_PHONE_NOCELL'] = data['CON_PHONE_NOCELL']
            dict_data['CON_WORK_PHONE'] = data['CON_WORK_PHONE']
            dict_data['DIVN_NAME'] = data['DIVN_NAME']
            dict_data['DLR_AREA'] = data['DLR_AREA']
            dict_data['DLR_BU'] = data['DLR_BU']
            dict_data['DLR_CODE'] = data['DLR_CODE']
            dict_data['DLR_LOC'] = data['DLR_LOC']
            dict_data['DLR_NAME'] = data['DLR_NAME']
            dict_data['DLR_ORG_CITY'] = data['DLR_ORG_CITY']
            dict_data['DLR_REGION'] = data['DLR_REGION']
            dict_data['DLR_SLS_STATE'] = data['DLR_SLS_STATE']

            if (data['LAST_SERVICE_DATE'] is not None) and (str(data['LAST_SERVICE_DATE']).isdigit()):
                dict_data['LAST_SERVICE_DATE'] = epoch_to_dt(data['LAST_SERVICE_DATE']).strftime('%d-%b-%Y')

            dict_data['PAR_ACCNT_ARN'] = data['PAR_ACCNT_ARN']
            dict_data['PAR_ACCNT_NAME'] = data['PAR_ACCNT_NAME']
            dict_data['PAR_ACCNT_SRC_ROW_ID'] = data['PAR_ACCNT_SRC_ROW_ID']
            dict_data['PAR_ACCNT_TYPE_CD'] = data['PAR_ACCNT_TYPE_CD']
            dict_data['SRC_ROW_WID'] = data['SRC_ROW_WID']
            dict_data['SR_INS_PRODUCT'] = data['SR_INS_PRODUCT']

            if (data['SR_OPEN_DT'] is not None) and (str(data['SR_OPEN_DT']).isdigit()):
                dict_data['SR_OPEN_DT'] = epoch_to_dt(data['SR_OPEN_DT']).strftime('%d-%b-%Y')

            dict_data['SR_REASON_CD'] = data['SR_REASON_CD']
            dict_data['SR_SR_NUM'] = data['SR_SR_NUM']
            dict_data['SR_STATUS'] = data['SR_STATUS']

            dict_data['_id'] = data['SRC_ROW_WID']
            dict_data['_index'] = index_name

            dict_data = {k: v for k, v in dict_data.items() if v is not None}
            details_data.append(dict_data)
        return details_data
    except Exception as err:
        print("DATA TRANSFORMATION ERROR :",err)
        traceback.print_exc()
        raise SystemExit


def index_creation_in_elastic_search(es,index_name):
    """ INDEX CREATION IN ELASTIC SEARCH """    
    try:
        print("INDEX CREATING...........")
        settings = {
                    "settings": {
                        "index.mapping.nested_objects.limit":10000000,
                        "index.refresh_interval" : "-1",
                        "index.max_inner_result_window" : 500000,
                        "index.max_result_window" : 500000,
                        "number_of_shards" : "3"
                    }
                }
        es.indices.create(index=index_name, ignore=400, body=settings)
    except Exception as err:
        print("INDEX CREATION ERROR,",err) 
        traceback.print_exc()
        raise SystemExit
    
def create_es_connection(es_credential):
    """Elastic Search Connection Establish"""
    try:
        print("CONNECTING TO ES.............")
        es = Elasticsearch(hosts=es_credential['host_address'], http_auth=(es_credential['user_name'],es_credential['password']), timeout=800, max_retries=10, retry_on_timeout=True)
        return es
    except Exception as err:
        print("ES CONNECTION ERROR",err)
        traceback.print_exc()
        raise SystemExit

def data_insertion_in_elastic_search(es,index_name,data):
    # Data insertion in elasticsearch
    try:
        print("DATA INSERTING INTO ES...........")
        helpers.bulk(es,data)
        es.indices.refresh(index=index_name)  
    except Exception as err:
        print("ERROR DATA INSERTION IN ELASTIC SEARCH",err)
        traceback.print_exc()
        raise SystemExit


if __name__ == '__main__':
    start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    print(f"Start time:",start_time.strftime('%Y-%m-%d %H:%M:%S'))

    index_name = sys.argv[1]

    try:
        conn = database_connection(db_credential) # database connection
        new_data = get_db_data(query, conn) # fetching data from db
        print(f"time_at_task",datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'))
        result = data_conversion(new_data) # data conversion
        print(f"time_at_task",datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'))
        transform_data = transform_new_data(index_name,result) # data transformation
        print(f"time_at_task",datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'))
        es = create_es_connection(es_credential) # es connection
        index_not_exists = not es.indices.exists(index=index_name) # check index exist or not
        if index_not_exists: 
            create_index = index_creation_in_elastic_search(es,index_name) # index creation in es
        data_insertion_in_elastic_search(es,index_name,transform_data) # data insertion into es
        
    except Exception as err:
        print("ERROR :",err)
        traceback.print_exc()

    end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    print(f"End time:",end_time.strftime('%Y-%m-%d %H:%M:%S'))
    print(f"Time taken: {(end_time - start_time).total_seconds():.2f} seconds")
    print("INDEX SUCCESSFULLY COMPLETED.......")
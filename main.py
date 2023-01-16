from kafka import KafkaConsumer,TopicPartition
import cx_Oracle
import sys
import json
import pandas as pd
import numpy as np
import time
import warnings
import config
warnings.simplefilter(action='ignore', category=FutureWarning)
from datetime import datetime

def write_log(stage,log_str,path,type):
	
    if type=='info':
        filename=path + type + "_" + str(datetime.now().strftime('%Y-%m-%d')) + ".txt"
    elif type=='error':
        filename=path + type + "_" + str(datetime.now().strftime('%Y-%m-%d')) + ".txt"
    else:
        filename=path + 'other_' + str(datetime.now().strftime('%Y-%m-%d')) + ".txt"
        
    f=open(filename,"a")
    message=str(stage) + " " + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ": "  + str(log_str)+"\n"
    f.write(message)
    f.close

def string_manipulation(s, suffix):
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    return s

def flatten_json(nested_json):

    out = {}
    i=0
    def flatten(x, name=''):
        global i
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')              
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

class oracle_db:
    def __init__(self,host,port,sid,user_name,password):
        self.host=host
        self.port=port
        self.sid=sid
        self.user_name=user_name
        self.password=password
    
    def execute(self,sql_statement,rows):
        dsn_tns = cx_Oracle.makedsn(self.host,self.port,self.sid)
        conn = cx_Oracle.connect(user=self.user_name, password=self.password, dsn=dsn_tns)
        c = conn.cursor()
        error_msg=[]
        c.executemany(sql_statement,rows,batcherrors=True)
        for error in c.getbatcherrors():
            msg="Error " + error.message + " at row offset" + str(error.offset)
            error_msg.append(msg)
            
        conn.commit()
        conn.close()
        return error_msg
    def getColumnNames(self,tablename,owner):
        dsn_tns = cx_Oracle.makedsn(self.host,self.port,self.sid)
        conn = cx_Oracle.connect(user=self.user_name, password=self.password, dsn=dsn_tns)
        c = conn.cursor()
        column_list=[]
        sqlStr="SELECT column_name FROM all_tab_cols "
        sqlStr+="WHERE table_name = '{}' AND owner = '{}'".format(tablename,owner)

        c.execute(sqlStr)
        for row in c:
            column_list.append(row[0])

        conn.close()
        return column_list



class sql_statement_maker:

    def __init__(self):
        pass

    def upsert(self,df,table_name: str,list_of_keys: list):
        
        columns=list(df)

        sql_statement="MERGE INTO {} USING DUAL ON (".format(table_name)

        if len(list_of_keys)==1:
            sql_statement+="{item}=:{item}".format(item=list_of_keys[0])
        elif len(list_of_keys)==0:
            print("please input key columns in list_of_keys varaible!")
        else:
            i=0
            for item in list_of_keys:
                
                if i==0:
                    sql_statement+="{item}=:{item}".format(item=item)
                else:
                    sql_statement+=" AND {item}=:{item}".format(item=item)
                i+=1
        sql_statement+=""")
        WHEN NOT MATCHED THEN INSERT(
        """
        str_values=""
        for item in columns:
            sql_statement+="{},".format(item)
            str_values+=":{},".format(item)

        sql_statement=string_manipulation(sql_statement,',')
        str_values=string_manipulation(str_values,',')
        sql_statement+=") VALUES ("+str_values+") WHEN MATCHED THEN UPDATE SET"

        value_columns = [item for item in columns if item not in list_of_keys]

        for item in value_columns:
            sql_statement+=" {item}=:{item},".format(item=item)


        sql_statement=string_manipulation(sql_statement,',')

        return sql_statement
    def insert(self,df,table_name: str):
        
        columns=list(df)
        sql_statement="INSERT INTO {} (".format(table_name)
        str_values=""
        for item in columns:
            sql_statement+="{},".format(item)
            str_values+=":{},".format(item)

        sql_statement=string_manipulation(sql_statement,',')
        str_values=string_manipulation(str_values,',')
        sql_statement+=") VALUES ("+str_values+") "
        sql_statement=string_manipulation(sql_statement,',')

        return sql_statement

def clean_column_name(df):
    for column in df.columns:
        if '-' or '/' in column:
            modified_column=column.replace('-','_')
            modified_column=modified_column.replace('/','_')
            df.rename(columns = {column:modified_column},inplace=True)
    return df
    
class kafka_consumer:
    def __init__(self,bootstrap_servers,topicName,kafka_group_id,timeout):
        self.bootstrap_servers=bootstrap_servers
        self.topicName=topicName
        self.kafka_group_id=kafka_group_id
        self.timeout=timeout

    def get_msg(self):
        consumer = KafkaConsumer(self.topicName,bootstrap_servers=self.bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        ,enable_auto_commit=False, group_id=self.kafka_group_id,consumer_timeout_ms=self.timeout)
        return consumer

def write_to_db(df,host,port,sid,user_name,password,table_name,log_path):
    df.fillna('', inplace=True)
    df = df.astype(str)
    clean_column_name(df)
    oracle_db_obj=oracle_db(host,port,sid,user_name,password)
    column_list=oracle_db_obj.getColumnNames("TBL_APPLICATION","SYSTEM")
    df.columns = df.columns.str.upper()
    df_list=list(df)
    df_to_dl=[]

    for item in df_list:
        if item in column_list:
            pass
        else:
            df_to_dl.append(item)

    if len(df_to_dl)>0:
        df=df.drop(df_to_dl,axis=1)
        new_col="New column not exist in table..."
        for col in df_to_dl:
            new_col+=col+', '
        write_log("New Column",new_col,log_path,'error')

    rows= df.to_dict(orient='records') 
    sql_statement_maker_obj=sql_statement_maker()
    sql_statement=sql_statement_maker_obj.insert(df,table_name)
    error_msg=oracle_db_obj.execute(sql_statement,rows)
    for msg in error_msg:
        write_log("batch errors",msg,log_path,'error')


table_name=config.table_name
bootsrap_server=config.kafka_bootstrap_servers
kafka_topic=config.kafka_topic
group_id=config.kafka_group_id
batch_size=config.batch_size
host=config.host
port=config.port
sid=config.sid
user_name=config.user_name
password=config.password
timeout=config.kafka_timeout
log_path=config.log_path

df_list=[]

consumer_obj=kafka_consumer(bootsrap_server,kafka_topic,group_id,timeout)
consumer=consumer_obj.get_msg()
write_log("start","....",log_path,'info')
i=1
offsets=[]

for msg in consumer:
    try:
        offsets.append(msg.offset)
        flatten_json_dict=flatten_json(msg.value)
        df_list.append(flatten_json_dict)
        
        i+=1
        if i > batch_size:
            break
    except Exception as e:
        write_log("flatten_json",e.__str__(),log_path,'error')
            
df=pd.DataFrame(df_list)       
write_log("df_count",len(df.index),log_path,'info')
inserted=False

while(inserted==False):
    try:
        write_to_db(df,host,port,sid,user_name,password,table_name,log_path)
        inserted=True
        write_log("write to db","Done!",log_path,'info')
    except Exception as e:
        write_log("write to db",e.__str__(),log_path,'error')
        time.sleep(10)
if len(offsets)>0:
    write_log("max offset",max(offsets),log_path,'info')

consumer.commit()
consumer.close()


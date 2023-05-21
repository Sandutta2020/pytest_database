import pytest
import cx_Oracle
import pandas as pd
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

engine = sqlalchemy.create_engine("oracle+cx_oracle://USERNAME:PASSWORD@HOSTNAME/?service_name=SERVICENAME", arraysize=1000) 
df =pd.read_csv('table_name_samecolumns.csv')
#print(df)
record_df = pd.DataFrame(columns=['table_name','column_name','SOURCEID','TARGETID','SOURCEVALUE','TARGETVALUE'])

def getting_record_for_test(table_name):
    try:
        Target_sql = "select rc.* from target_schema."+table_name+" rc,source_schema."+table_name+" c where rc.source_id__c = c.id and rownum <2"""; 
        #df_campaign = pd.read_sql(campaign_sql, engine)
        df_target = pd.read_sql_query(sql= text(Target_sql), con=engine.connect())
        source_id= ','.join("'"+x+"'" for x in df_target['source_id__c'])
        Source_sql = "SELECT * FROM " + table_name+' where id in('+source_id+')'; 
        #df_campaign = pd.read_sql(campaign_sql, engine)
        df_source = pd.read_sql_query(sql= text(Source_sql), con=engine.connect())
        #print(df_target.head())
        df_source=df_source.fillna('00X00')
        df_target=df_target.fillna('00X00')
        return df_source,df_target
    except SQLAlchemyError as e:
        print(e)
def getting_similar_columns(table_name):
    try:
        common_columns_sql = "select lower(COLUMN_NAME) as COLUMN_NAME,table_name from all_tab_columns where owner='target_schema' and lower(table_name )='{table_name_s}' and column_name not like'%ID' INTERSECT select lower(COLUMN_NAME) as COLUMN_NAME,table_name from all_tab_columns where owner='source_schema' and lower(table_name)='{table_name_s}'".format(table_name_s=table_name)
        #print(common_columns_sql)
        df_common_columns= pd.read_sql_query(sql= text(common_columns_sql), con=engine.connect())
        return df_common_columns
    except SQLAlchemyError as e:
        print(e)
for table in df['table_name'].values:
    #print(table)
    df_same_cols  =getting_similar_columns(table)
    #print(df_same_cols.shape)    
    df_source,df_target =getting_record_for_test(table)
    for sim_columns in df_same_cols['column_name'].values:
        for index, row in df_target.iterrows():
            source_val =df_source[df_source['id']==row["source_id__c"]][[sim_columns]].values[0][0]
            target_val =df_target[df_target['id']==row["id"]][[sim_columns]].values[0][0] 
            #print(row["id"], row["source_id__c"],sim_columns,source_val,target_val)
            dict_r ={'table_name':table,'column_name':sim_columns,'SOURCEID':row["source_id__c"],'TARGETID':row["id"],'SOURCEVALUE':source_val,'TARGETVALUE':target_val} 
            #print(dict_r)
            record_df = record_df.append(dict_r, ignore_index = True)          
print(record_df.shape) 
record_df.to_csv('record_csv.csv')

@pytest.mark.parametrize("column_name,table_name,SOURCEID,TARGETID,SOURCEVALUE,TARGETVALUE",list(record_df[['column_name','table_name','SOURCEID','TARGETID','SOURCEVALUE','TARGETVALUE']].values))
def test_simiar_coumn_data(column_name,table_name,SOURCEID,TARGETID,SOURCEVALUE,TARGETVALUE):
    assert SOURCEVALUE ==TARGETVALUE

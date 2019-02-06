from sqlalchemy.orm import sessionmaker
from sqlalchemy import *
from io import StringIO
import pandas as pd
import numpy as np
    

def merge_load(load_id,engine,srctable,tgttable):
    
    conn = engine.raw_connection()
    cur = conn.cursor()
    colquery = "select string_agg(column_name,',') \
                    from information_schema.columns \
                    where lower(table_schema) = '{schema}' and lower(table_name)='{table}' \
                    and lower(column_name) not in ('load_id','load_dttm','rowstatus')"
    pkcolquery = "select column_name pkey \
                    from information_schema.key_column_usage c \
                    left join information_schema.table_constraints  t ON t.constraint_name = c.constraint_name \
                    where lower(c.table_name) = '{table}' \
                    and lower(c.table_schema)='{schema}' \
                    and upper(t.constraint_type) = 'PRIMARY KEY'"
    del_query = "delete from {tgttbl} where {pkcols} in (select {pkcols} from {srctbl} where load_id={load_id})"
    ins_query = "insert into {tgttbl} ({tgtcols},load_id,load_dttm,rowstatus) \
                    select {srccols},{load_id},current_date load_dttm,1 rowstatus from {srctbl} where load_id={load_id}"

    try:
        cur.execute(colquery.format(schema = tgttable.__table_args__['schema'], table = tgttable.__tablename__))
        tgtcols = cur.fetchone()
    except Exception as error:
        print('MERGE_LOAD: Failed to select tgt columns!' + str(error))

    try:
        cur.execute(colquery.format(schema = srctable.__table_args__['schema'], table = srctable.__tablename__))
        srccols = cur.fetchone()
    except Exception as error:
        print('MERGE_LOAD: Failed to select src columns!' + str(error))

    try:
        cur.execute(pkcolquery.format(schema = tgttable.__table_args__['schema'], table = tgttable.__tablename__))
        pkcols = cur.fetchone()
    except Exception as error:
        print('MERGE_LOAD: Failed to select PK columns!' + str(error))

    try:
        cur.execute(del_query.format(tgttbl = tgttable.__table_args__['schema']+'.'+tgttable.__tablename__,\
                                pkcols = pkcols[0],\
                                srctbl = srctable.__table_args__['schema']+'.'+srctable.__tablename__,\
                                load_id = load_id))
    except Exception as error:
        print('MERGE_LOAD: Failed to delete records from tgt! ' + str(error))

    try:
        cur.execute(ins_query.format(tgttbl = tgttable.__table_args__['schema']+'.'+tgttable.__tablename__,\
                            tgtcols = tgtcols[0],\
                            srccols = srccols[0],\
                            srctbl = srctable.__table_args__['schema']+'.'+srctable.__tablename__,\
                            load_id = load_id))
        print('MERGE_LOAD: Upload of {table} has been finished successfully!'.format(table = tgttable.__table_args__['schema']+'.'+tgttable.__tablename__))
    except Exception as error:
        print('MERGE_LOAD: Failed to insert new records from tgt! ' + str(error))
    conn.commit()
    cur.close()

def del_by_load_id(load_id,engine,table):
    
    Session = sessionmaker(engine)
    session = Session()
    session.query(table).filter(table.load_id == load_id).delete()
    session.commit()
    session.close()
    
def bulk_load(load_id,engine,table,df_data,sep):
    
    connection = engine.raw_connection()
    del_by_load_id(load_id,engine,table)
    data_csv = StringIO()
    df_data['load_id'] = load_id
    df_data['load_dttm'] = np.nan
    pd.to_datetime(df_data.load_dttm)
    df_data.to_csv(data_csv, sep=sep, header=False, index=False)
    data_csv.seek(0)
    contents = data_csv.getvalue()
    cur = connection.cursor()
    try:
        cur.copy_from(data_csv, table.__table_args__['schema']+'.'+table.__tablename__,sep=sep, null='') 
        connection.commit()
    except Exception as error:
        print('Bulk Load: Failed to insert new records to {table}! '.format(table = table.__table_args__['schema']+'.'+table.__tablename__) + str(error))
    cur.close()


def str2hash(text):
    import hashlib
    return hashlib.sha1(text.encode('utf-8')).hexdigest()

def booltoint(text):
    return 1 if text=='true' else 0
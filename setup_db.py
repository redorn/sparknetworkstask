import sys
import logging
import psycopg2
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import *
import datetime
from io import StringIO
import db_config
from variables import *
from ext_tblmetadata import *
from dwh_tblmetadata import *
from stg_tblmetadata import *
 
def setup_db(conn):
    try:
        return create_engine(conn)
    except:
        print("ERROR: Could not connect to redorndb!")
        sys.exit()
    
if __name__ == "__main__":
    
    engine = setup_db(db_config.db_conn)
    
    try:
        engine.execute("CREATE SCHEMA IF NOT EXISTS stg")
        engine.execute("CREATE SCHEMA IF NOT EXISTS stg2")
        engine.execute("CREATE SCHEMA IF NOT EXISTS dwh")
    
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        
        print("Metadata has been created succesfully!")
    
    except Exception as e:
        print("Metadata creation has been failed!" + str(e))
        sys.exit()
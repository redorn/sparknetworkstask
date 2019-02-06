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
from setup_db import *
from extract import *
from transform import *
from pkg_etl_utils import *

if __name__ == "__main__":
    
    engine = setup_db(db_config.db_conn)
    ext_flat_json(load_id,engine,'immobilienscout24_berlin_20190113.json',JData)
    transform(load_id,engine,JData)
    merge_load(load_id,engine,stg_apartmentype,d_apartmentype)
    merge_load(load_id,engine,stg_totalrentscope,d_totalrentscope)
    merge_load(load_id,engine,stg_interiortype,d_interiortype)
    merge_load(load_id,engine,stg_heatingtype,d_heatingtype)
    merge_load(load_id,engine,stg_petsallowed,d_petsallowed)
    merge_load(load_id,engine,stg_company,d_company)
    merge_load(load_id,engine,stg_country,d_country)
    merge_load(load_id,engine,stg_region,d_region)
    merge_load(load_id,engine,stg_city,d_city)
    merge_load(load_id,engine,stg_district,d_district)
    merge_load(load_id,engine,stg_address,d_address)
    merge_load(load_id,engine,stg_flat,f_flat)
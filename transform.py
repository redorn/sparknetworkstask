import sys
import logging
import psycopg2
import pandas as pd
import numpy as np
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
from pkg_etl_utils import *

def transform(load_id,engine,ext_data):
    
    try:
        df_jdata = pd.read_sql('select * from {j_table}'.format(j_table = ext_data.__table_args__['schema']+'.'+ext_data.__tablename__), engine)
        df_jdata['modification']=pd.to_datetime(df_jdata.modification, format ="%Y%m%d %H:%M:%S")
        df_jdata['creation']=pd.to_datetime(df_jdata.creation, format ="%Y%m%d %H:%M:%S")
        df_jdata['publishdate']=pd.to_datetime(df_jdata.publishdate, format ="%Y%m%d %H:%M:%S")
        print('Transform: STG data has been uploaded to DataFrame!')
    except Exception as e:
        print("Transform: STG data unload has been failed!" + str(e))
        sys.exit()

    try:
        city_df=pd.DataFrame(df_jdata, columns =['city_id','realestate_address_geohierarchy_city_name', 
                                    'code',
                                    'realestate_address_geohierarchy_city_geocodeid',
                                    'realestate_address_geohierarchy_city_fullgeocodeid',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        city_df['city_id'] = city_df['realestate_address_geohierarchy_city_name'].apply(lambda x: str(str2hash(x)))
        city_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_city,city_df,';')
        del city_df
        print('Transform: City data has been loaded to STG2!')
    except Exception as e:
        print("Transform: City data transformation has been failed!" + str(e))
        sys.exit()

    
    try:
        district_df=pd.DataFrame(df_jdata, columns =['district_id','realestate_address_geohierarchy_quarter_name',
                                    'code',
                                    'realestate_address_geohierarchy_quarter_geocodeid',
                                    'realestate_address_geohierarchy_quarter_fullgeocodeid',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        district_df['district_id'] = district_df['realestate_address_geohierarchy_quarter_name'].apply(lambda x: str(str2hash(x)))
        district_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_district,district_df,';')
        del district_df
        print('Transform: District data has been loaded to STG2!')
    except Exception as e:
        print("Transform: District data transformation has been failed!" + str(e))
        sys.exit()
        
    try:
        region_df=pd.DataFrame(df_jdata, columns =['region_id','realestate_address_geohierarchy_region_name',
                                    'realestate_address_geohierarchy_region_geocodeid',
                                    'realestate_address_geohierarchy_region_fullgeocodeid',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        region_df['region_id'] = region_df['realestate_address_geohierarchy_region_name'].apply(lambda x: str(str2hash(x)))
        region_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_region,region_df,';')
        del region_df
        print('Transform: Region data has been loaded to STG2!')
    except Exception as e:
        print("Transform: Region data transformation has been failed!" + str(e))
        sys.exit()

    try:
        country_df=pd.DataFrame(df_jdata, columns =['country_id','realestate_address_geohierarchy_country_name',
                                    'realestate_address_geohierarchy_country_geocodeid',
                                    'realestate_address_geohierarchy_country_fullgeocodeid',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        country_df['country_id'] = country_df['realestate_address_geohierarchy_country_name'].apply(lambda x: str(str2hash(x)))
        country_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_country,country_df,';')
        del country_df
        print('Transform: Country data has been loaded to STG2!')
    except Exception as e:
        print("Transform: Country data transformation has been failed!" + str(e))
        sys.exit()

    try:
        interiortype_df=pd.DataFrame(df_jdata, 
                                     columns = ['interiortype_id','realestate_interiorquality',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates().dropna(thresh=2)
        interiortype_df['interiortype_id'] = interiortype_df['realestate_interiorquality'].apply(lambda x: str(str2hash(x)))
        interiortype_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_interiortype,interiortype_df,';')
        del interiortype_df
        print('Transform: Interiortype data has been loaded to STG2!')
    except Exception as e:
        print("Transform: Interiortype data transformation has been failed!" + str(e))
        sys.exit()

    try:
        heatingtype_df=pd.DataFrame(df_jdata, 
                                     columns = ['heatingtype_id','realestate_heatingtype',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates().dropna(thresh=2)
        heatingtype_df['heatingtype_id'] = heatingtype_df['realestate_heatingtype'].apply(lambda x: str(str2hash(x)))
        heatingtype_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_heatingtype,heatingtype_df,';')
        del heatingtype_df
        print('Transform: HeatingType data has been loaded to STG2!')
    except Exception as e:
        print("Transform: HeatingType data transformation has been failed!" + str(e))
        sys.exit()

    try:
        aprttype_df=pd.DataFrame(df_jdata, columns =['apttype_id','realestate_apartmenttype',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        aprttype_df['apttype_id'] = aprttype_df['realestate_apartmenttype'].apply(lambda x: str(str2hash(x)))
        aprttype_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_apartmentype,aprttype_df,';')
        del aprttype_df
        print('Transform: ApartmentType data has been loaded to STG2!')
    except Exception as e:
        print("Transform: ApartmentType data transformation has been failed!" + str(e))
        sys.exit()

    try:
        petsallowed_df=pd.DataFrame(df_jdata, columns =['petsallowed_id','realestate_petsallowed',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        petsallowed_df['petsallowed_id'] = petsallowed_df['realestate_petsallowed'].apply(lambda x: str(str2hash(x)))
        petsallowed_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_petsallowed,petsallowed_df,';')
        del petsallowed_df
        print('Transform: PetsAllowedType data has been loaded to STG2!')
    except Exception as e:
        print("Transform: PetsAllowedType data transformation has been failed!" + str(e))
        sys.exit()

    try:
        totalrentscope_df=pd.DataFrame(df_jdata, columns =['totalrentscope_id','realestate_calculatedtotalrentscope',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ]).drop_duplicates()
        totalrentscope_df['totalrentscope_id'] = totalrentscope_df['realestate_calculatedtotalrentscope'].apply(lambda x: str(str2hash(x)))
        totalrentscope_df['rowstatus'] = 1
        bulk_load(load_id,engine,stg_totalrentscope,totalrentscope_df,';')
        del totalrentscope_df
        print('Transform: TotalRentScopeType data has been loaded to STG2!')
    except Exception as e:
        print("Transform: TotalRentScopeType data transformation has been failed!" + str(e))
        sys.exit()

    try:
        company_df=pd.DataFrame(df_jdata.groupby('contactdetails_company', as_index = False).first(), 
                                    columns =['company_id',
                                    'contactdetails_company',
                                    'contactdetails_homepageurl',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ])
        company_df['company_id'] = company_df['contactdetails_company'].apply(lambda x: str(str2hash(str(x))))
        company_df['rowstatus'] = 1
        #company_df.head(100)
        bulk_load(load_id,engine,stg_company,company_df,';')
        del company_df
        print('Transform: Company data has been loaded to STG2!')
    except Exception as e:
        print("Transform: Company data transformation has been failed!" + str(e))
        sys.exit()

    try:
        address_df=pd.DataFrame(df_jdata.groupby('realestate_address_postcode', as_index = False).first(), 
                                    columns =['address_id',
                                    'realestate_address_postcode',
                                    'street',
                                    'housenumber',
                                    'realestate_address_geohierarchy_quarter_name',
                                    'realestate_address_geohierarchy_city_name',
                                    'realestate_address_geohierarchy_region_name',
                                    'realestate_address_geohierarchy_country_name',
                                    'continent_id',
                                    'longitude',
                                    'latitude',
                                    'load_id',
                                    'load_dttm',          
                                    'rowstatus'
                                   ])
        address_df['address_id'] = address_df['realestate_address_postcode'].apply(lambda x: str(str2hash(str(x))))
        address_df['realestate_address_geohierarchy_city_name'] = address_df['realestate_address_geohierarchy_city_name'].apply(lambda x: str(str2hash(str(x))))
        address_df['realestate_address_geohierarchy_quarter_name'] = address_df['realestate_address_geohierarchy_quarter_name'].apply(lambda x: str(str2hash(str(x))))
        address_df['realestate_address_geohierarchy_region_name'] = address_df['realestate_address_geohierarchy_region_name'].apply(lambda x: str(str2hash(str(x))))
        address_df['realestate_address_geohierarchy_country_name'] = address_df['realestate_address_geohierarchy_country_name'].apply(lambda x: str(str2hash(str(x))))
        address_df['continent_id'] = str(str2hash("-2"))
        address_df['rowstatus'] = 1
        #address_df.head(100)
        bulk_load(load_id,engine,stg_address,address_df,';')
        del address_df
        print('Transform: Address data has been loaded to STG2!')
    except Exception as e:
        print("Transform: Address data transformation has been failed!" + str(e))
        sys.exit()

    try:
        flat_df=pd.DataFrame(df_jdata.groupby('realestate_id', as_index = False).first(), 
                                    columns =['flat_id',
                                    'contactdetails_company',
                                    'contactdetails_cellphonenumber',
                                    'realestate_address_postcode',
                                    'realestate_id',
                                    'realestate_apartmenttype',
                                    'realestate_balcony',
                                    'realestate_builtinkitchen',
                                    'realestate_calculatedtotalrentscope',
                                    'realestate_constructionyear',
                                    'realestate_floor',
                                    'realestate_garden',
                                    'realestate_interiorquality',
                                    'realestate_heatingtype',
                                    'realestate_baserent',
                                    'realestate_calculatedtotalrent',
                                    'publishdate',
                                    'realestate_lastmodificationdate',
                                    'realestate_lift',
                                    'realestate_numberofrooms',
                                    'realestate_numberoffloors',
                                    'realestate_petsallowed',
                                    'isactive',
                                    'realestate_servicecharge',
                                    'realestate_totalrent',
                                    'realestate_livingspace',
                                    'load_id',
                                    'load_dttm',
                                    'rowstatus'
                                   ])
        flat_df['flat_id'] = flat_df['realestate_id'].apply(lambda x: str(str2hash(str(x))))
        flat_df['contactdetails_company'] = flat_df['contactdetails_company'].apply(lambda x: str(str2hash(str(x))))
        flat_df['contactdetails_cellphonenumber'] = flat_df['contactdetails_cellphonenumber'].apply(lambda x: str(str2hash(str(x).strip().replace(" ",""))))
        flat_df['realestate_address_postcode'] = flat_df['realestate_address_postcode'].apply(lambda x: str(str2hash(str(x))))
        flat_df['realestate_apartmenttype'] = flat_df['realestate_apartmenttype'].apply(lambda x: str(str2hash(str(x))))
        flat_df['realestate_calculatedtotalrentscope'] = flat_df['realestate_calculatedtotalrentscope'].apply(lambda x: str(str2hash(str(x))))
        flat_df['realestate_interiorquality'] = flat_df['realestate_interiorquality'].apply(lambda x: str(str2hash(str(x))))
        flat_df['realestate_heatingtype'] = flat_df['realestate_heatingtype'].apply(lambda x: str(str2hash(str(x))))
        flat_df['realestate_petsallowed'] = flat_df['realestate_petsallowed'].apply(lambda x: str(str2hash(str(x))))
        flat_df['rowstatus'] = 1

        flat_df['realestate_balcony'] = flat_df['realestate_balcony'].apply(lambda x: booltoint(x))
        flat_df['realestate_builtinkitchen'] = flat_df['realestate_builtinkitchen'].apply(lambda x: booltoint(x))
        flat_df['realestate_garden'] = flat_df['realestate_garden'].apply(lambda x: booltoint(x))
        flat_df['realestate_lift'] = flat_df['realestate_lift'].apply(lambda x: booltoint(x))

        flat_df['realestate_baserent'] = flat_df['realestate_baserent'].apply(lambda x: float(x))
        flat_df['realestate_calculatedtotalrent'] = flat_df['realestate_calculatedtotalrent'].apply(lambda x: float(x))
        flat_df['realestate_totalrent'] = flat_df['realestate_totalrent'].apply(lambda x: float(x))
        flat_df['realestate_servicecharge'] = flat_df['realestate_servicecharge'].apply(lambda x: float(x))

        flat_df['realestate_constructionyear'] = flat_df['realestate_constructionyear'].apply(lambda x: float(x))
        flat_df['realestate_floor'] = flat_df['realestate_floor'].apply(lambda x: float(x))
        flat_df['realestate_numberofrooms'] = flat_df['realestate_numberofrooms'].apply(lambda x: int(float(x)) if pd.notnull(x) else x)
        flat_df['realestate_numberoffloors'] = flat_df['realestate_numberoffloors'].apply(lambda x: int(float(x)) if pd.notnull(x) else x)
        flat_df['realestate_livingspace'] = flat_df['realestate_livingspace'].apply(lambda x: int(float(x)) if pd.notnull(x) else x)

        #pd.set_option('display.max_columns', 100)
        #flat_df.dtypes
        #flat_df.head(100)
        bulk_load(load_id,engine,stg_flat,flat_df,';')
        del flat_df
        print('Transform: Flat data has been loaded to STG2!')
    except Exception as e:
        print("Transform: Flat data transformation has been failed!" + str(e))
        sys.exit()
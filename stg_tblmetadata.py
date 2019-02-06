import sys
from sqlalchemy import *
import datetime
from variables import Base

#global Base = declarative_base()

class stg_continent(Base):
    __tablename__ = "stg_continent"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    geocode = Column(BigInteger)
    fullgeocode = Column(BigInteger)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)
    
class stg_region(Base):
    __tablename__ = "stg_region"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    geocode = Column(BigInteger)
    fullgeocode = Column(BigInteger)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)

class stg_country(Base):
    __tablename__ = "stg_country"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    geocode = Column(BigInteger)
    fullgeocode = Column(BigInteger)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)

class stg_district(Base):
    __tablename__ = "stg_district"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    code = Column(String)
    geocode = Column(BigInteger)
    fullgeocode = Column(BigInteger)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)
    
class stg_city(Base):
    __tablename__ = "stg_city"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    code = Column(String)
    geocode = Column(BigInteger)
    fullgeocode = Column(BigInteger)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)

class stg_address(Base):
    __tablename__ = "stg_address"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    postcode = Column(String)
    street = Column(String)
    housenumber = Column(String)
    district_id = Column (LargeBinary)
    city_id = Column(LargeBinary)
    region_id  = Column(LargeBinary)
    country_id = Column(LargeBinary)
    continent_id = Column(LargeBinary)
    longitude = Column(Float)
    latitude = Column(Float)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)

class stg_contact(Base):
    __tablename__ = "stg_contact"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    cellphonenum = Column(String)
    email = Column(String)
    faxnumber = Column(String)
    firstname = Column (String)
    lastname = Column(String)
    phonenumber  = Column(String)
    sex = Column(String)
    address_id = Column(LargeBinary)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)
    
class stg_company(Base):
    __tablename__ = "stg_company"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    homepageurl = Column(String)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)
    
class stg_flat(Base):
    __tablename__ = "stg_flat"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    company_id = Column(LargeBinary)
    contact_id = Column(LargeBinary)
    address_id = Column(LargeBinary)
    external_id = Column(String)
    apartmenttypeid = Column(LargeBinary)
    isbalcony= Column(Integer)
    isbuitinkitchen= Column(Integer)
    totalrentscope_id = Column(LargeBinary)
    constructionyear= Column(Float)
    floornum= Column(Float)
    isgarden= Column(Integer)
    interiortype_id = Column(LargeBinary)
    heatingtype_id = Column(LargeBinary)
    baserenteur =Column(Float)
    calctotalrenteur=Column(Float)
    publishdate=Column(DateTime)
    lastmodificationdate=Column(DateTime)
    islift= Column(Integer)
    rooms= Column(Integer)
    floors= Column(Float)
    petsallowed_id = Column(LargeBinary)
    isactive= Column(Integer)
    servicechargeeur=Column(Float)
    totalrenteur= Column(Float)
    livingspace= Column(Integer)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)

class stg_interiortype(Base):
    __tablename__ = "stg_interiortype"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)
    
class stg_heatingtype(Base):
    __tablename__ = "stg_heatingtype"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)

class stg_apartmentype(Base):
    __tablename__ = "stg_apartmentype"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime)
    rowstatus = Column(Integer)


class stg_totalrentscope(Base):
    __tablename__ = "stg_totalrentscope"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)


class stg_petsallowed(Base):
    __tablename__ = "stg_petsallowed"
    __table_args__ = ({"schema": "stg2"})
    id = Column(LargeBinary, primary_key  = True)
    name = Column(String)
    load_id = Column(Integer, primary_key = True)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    rowstatus = Column(Integer)
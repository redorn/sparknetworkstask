import sys
import datetime
from sqlalchemy import *
from variables import Base


class JData(Base):
    __tablename__ = "j_data"
    __table_args__ = ({"schema": "stg"})
    contactformtype = Column(String)
    creation = Column(String)
    id = Column(Integer, primary_key  = true)
    modification = Column(String)
    publishdate = Column(String)
    xlink_href = Column(String)
    adlinkforjsonp_xlink_href = Column(String)
    adlinkforxmldata_xlink_href = Column(String)
    companywidecustomerid = Column(String)
    contactdetails_id = Column(String)
    contactdetails_address_city = Column(String)
    contactdetails_address_housenumber = Column(String)
    contactdetails_address_postcode = Column(String)
    contactdetails_address_street = Column(String)
    contactdetails_cellphonenumber = Column(String)
    contactdetails_cellphonenumberareacode = Column(String)
    contactdetails_cellphonenumbercountrycode = Column(String)
    contactdetails_cellphonenumbersubscriber = Column(String)
    contactdetails_company = Column(String)
    contactdetails_countrycode = Column(String)
    contactdetails_email = Column(String)
    contactdetails_faxnumber = Column(String)
    contactdetails_faxnumberareacode = Column(String)
    contactdetails_faxnumbercountrycode = Column(String)
    contactdetails_faxnumbersubscriber = Column(String)
    contactdetails_firstname = Column(String)
    contactdetails_homepageurl = Column(String)
    contactdetails_lastname = Column(String)
    contactdetails_phonenumber = Column(String)
    contactdetails_phonenumberareacode = Column(String)
    contactdetails_phonenumbercountrycode = Column(String)
    contactdetails_phonenumbersubscriber = Column(String)
    contactdetails_salutation = Column(String)
    contactformconfiguration_addressfield = Column(String)
    contactformconfiguration_applicationpackagecompletedfield = Column(String)
    contactformconfiguration_emailaddressfield = Column(String)
    contactformconfiguration_employmentrelationshipfield = Column(String)
    contactformconfiguration_firstnamefield = Column(String)
    contactformconfiguration_freemiumsettings_duration = Column(String)
    contactformconfiguration_incomefield = Column(String)
    contactformconfiguration_lastnamefield = Column(String)
    contactformconfiguration_messagefield = Column(String)
    contactformconfiguration_moveindatefield = Column(String)
    contactformconfiguration_numberofpersonsfield = Column(String)
    contactformconfiguration_petsinhouseholdfield = Column(String)
    contactformconfiguration_phonenumberfield = Column(String)
    contactformconfiguration_premiumprofilerequired = Column(String)
    contactformconfiguration_salutationfield = Column(String)
    imprintlink_xlink_href = Column(String)
    realestate_id = Column(String)
    realestate_xsi_type = Column(String)
    realestate_address_city = Column(String)
    realestate_address_geohierarchy_city_fullgeocodeid = Column(String)
    realestate_address_geohierarchy_city_geocodeid = Column(String)
    realestate_address_geohierarchy_city_name = Column(String)
    realestate_address_geohierarchy_continent_fullgeocodeid = Column(String)
    realestate_address_geohierarchy_continent_geocodeid = Column(String)
    realestate_address_geohierarchy_country_fullgeocodeid = Column(String)
    realestate_address_geohierarchy_country_geocodeid = Column(String)
    realestate_address_geohierarchy_country_name = Column(String)
    realestate_address_geohierarchy_neighbourhood_geocodeid = Column(String)
    realestate_address_geohierarchy_quarter_fullgeocodeid = Column(String)
    realestate_address_geohierarchy_quarter_geocodeid = Column(String)
    realestate_address_geohierarchy_quarter_name = Column(String)
    realestate_address_geohierarchy_region_fullgeocodeid = Column(String)
    realestate_address_geohierarchy_region_geocodeid = Column(String)
    realestate_address_geohierarchy_region_name = Column(String)
    realestate_address_postcode = Column(String)
    realestate_address_quarter = Column(String)
    realestate_apartmenttype = Column(String)
    realestate_assistedliving = Column(String)
    realestate_attachments = Column(String)
    realestate_balcony = Column(String)
    realestate_baserent = Column(String)
    realestate_buildingenergyratingtype = Column(String)
    realestate_builtinkitchen = Column(String)
    realestate_calculatedtotalrent = Column(String)
    realestate_calculatedtotalrentscope = Column(String)
    realestate_cellar = Column(String)
    realestate_certificateofeligibilityneeded = Column(String)
    realestate_condition = Column(String)
    realestate_constructionyear = Column(String)
    realestate_courtage_hascourtage = Column(String)
    realestate_creationdate = Column(String)
    realestate_deposit = Column(String)
    realestate_descriptionnote = Column(String)
    realestate_energycertificate_energycertificateavailability = Column(String)
    realestate_energycertificate_energycertificatecreationdate = Column(String)
    realestate_energyconsumptioncontainswarmwater = Column(String)
    realestate_energyperformancecertificate = Column(String)
    realestate_externalid = Column(String)
    realestate_floor = Column(String)
    realestate_floorplan = Column(String)
    realestate_freefrom = Column(String)
    realestate_furnishingnote = Column(String)
    realestate_garden = Column(String)
    realestate_guesttoilet = Column(String)
    realestate_handicappedaccessible = Column(String)
    realestate_heatingcosts = Column(String)
    realestate_heatingcostsinservicecharge = Column(String)
    realestate_heatingtype = Column(String)
    realestate_heatingtypeenev2014 = Column(String)
    realestate_interiorquality = Column(String)
    realestate_lastmodificationdate = Column(String)
    realestate_lastrefurbishment = Column(String)
    realestate_lift = Column(String)
    realestate_livingspace = Column(String)
    realestate_locationnote = Column(String)
    realestate_numberoffloors = Column(String)
    realestate_numberofrooms = Column(String)
    realestate_othernote = Column(String)
    realestate_petsallowed = Column(String)
    realestate_referencepriceapicall = Column(String)
    realestate_referencepriceservicecall = Column(String)
    realestate_servicecharge = Column(String)
    realestate_state = Column(String)
    realestate_thermalcharacteristic = Column(String)
    realestate_title = Column(String)
    realestate_titlepicture_creation = Column(String)
    realestate_titlepicture_id = Column(String)
    realestate_titlepicture_modification = Column(String)
    realestate_titlepicture_publishdate = Column(String)
    realestate_titlepicture_floorplan = Column(String)
    realestate_titlepicture_title = Column(String)
    realestate_titlepicture_titlepicture = Column(String)
    realestate_titlepicture_urls = Column(String)
    realestate_totalrent = Column(String)
    realestate_useasflatshareroom = Column(String)
    realtorvaluationjsonlink_xlink_href = Column(String)
    realtorvaluationv2jsonlink_xlink_href = Column(String)
    realtorvaluationv2jsonplink_xlink_href = Column(String)
    load_id = Column(Integer)
    load_dttm = Column(DateTime, default = datetime.datetime.utcnow)
    
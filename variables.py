from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
load_id = 0
global sep
sep = ";"
#config file containing credentials for DB instance
db_username = "redorn"
db_password = "KFD5kKJj"
db_name = "redorndb" 
db_port = "5432"
db_host = "redorndb.cdpish4uitus.eu-west-1.rds.amazonaws.com"
db_conn = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_host+':'+db_port+'/'+db_name
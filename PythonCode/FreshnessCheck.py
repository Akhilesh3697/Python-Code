from utilities import *
from properties import *
from logfiles import *
from datetime import datetime
from datetime import date

hour='-1'

today = date.today()
# logger=get_logger()

def get_dataPresent(src_connection, query, tablename):
    try:
        cursor = src_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        cursor.close()

        if result > 0:
            # logger.info(f"Data is present in the table {tablename}")
            print(f"Data is present in the table {tablename}")
            with open(f'LogFiles\\DatainFreshnessCheck_{today}.log', 'a') as f:
                f.write(f"{tablename} table has data\n")
        else:
            # logger.info(f"No data found in the table {tablename}")
            print(f"No data found in the table {tablename}")
            with open(f'LogFiles\\NoDatainFreshnessCheck_{today}.log', 'a') as f:
                f.write(f"{tablename} table has no data\n")
    except pyodbc.Error as e:
        logger.error(f"Error connecting to SQL Server: {e}")

def get_columnDataNull(src_connection, query, tablename):
    try:
        cursor = src_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        cursor.close()

        if result == 0 or result is None:
            print(f"There are no null records in the SystemModifiedDate column for {tablename} table")
            with open(f'LogFiles\\DatainFreshnessCheck_{today}.log', 'a') as f:
                f.write(f"There are no null records in the SystemModifiedDate column for {tablename} table\n")
        else:
            print(f"There are {result} null records in the SystemModifiedDate column for {tablename} table")
            with open(f'LogFiles\\NoDatainFreshnessCheck_{today}.log', 'a') as f:
                f.write(f"There are {result} null records in the SystemModifiedDate column for {tablename} table\n")
    except pyodbc.Error as e:
        logger.error(f"Error connecting to SQL Server: {e}")

# ******************************************************************************************************************    
GFWDataMart_database=''
src_connection= connectionSQLUnandPw(nexus_sql_servername,GFWDataMart_database,nexus_sql_username,nexus_sql_password)
print(f'Successfully connected to Database={GFWDataMart_database}')
GFWDataMart_tables=['a','b','c','d']
for tablename in GFWDataMart_tables:
    if tablename == 'a':
        query_GFWDataMart = f"""SELECT count(*) FROM {GFWDataMart_database}.dbo.{tablename} WHERE SystemModifiedDate >= DATEADD(hour, {hour}, GETDATE())"""
        get_dataPresent(src_connection, query_GFWDataMart, tablename)
        query_GFWDataMart2 = f"""select count(*) from {GFWDataMart_database}.dbo.{tablename} where SystemModifiedDate >=(GETDATE(){hour}) and SystemModifiedDate is NULL"""
        get_columnDataNull(src_connection, query_GFWDataMart2, tablename)
    elif tablename == 'b':
        query_GFWDataMart = f"""SELECT count(*) FROM {GFWDataMart_database}.dbo.{tablename} WHERE SystemModifiedDate >= DATEADD(day, {hour}, GETDATE())"""
        get_dataPresent(src_connection, query_GFWDataMart, tablename)
    else:
        query_GFWDataMart = f"""SELECT count(*) FROM {GFWDataMart_database}.dbo.{tablename} WHERE SystemModifiedDate >= DATEADD(hour, {hour}, GETDATE())"""
        get_dataPresent(src_connection, query_GFWDataMart, tablename)
        
# *****************************************************************************************************************
PSI_Database=''
src_connection= connectionSQLUnandPw(nexus_sql_servername,PSI_Database,nexus_sql_username,nexus_sql_password)
print(f'Successfully connected to Database={PSI_Database}')
PSI_tablenames = ['a','','b']
for tablename in PSI_tablenames:
    if tablename == 'a':
        query_PSI = f"""SELECT count(*) FROM {PSI_Database}.dbo.{tablename} WHERE SystemCreatedDate >= DATEADD(hour, {hour}, GETDATE())"""
        get_dataPresent(src_connection, query_PSI, tablename)
    else:
        query_PSI = f"""SELECT count(*) FROM {PSI_Database}.dbo.{tablename} WHERE SystemCreatedDate >= DATEADD(day, {hour}, GETDATE())"""
        get_dataPresent(src_connection, query_PSI, tablename)

# *****************************************************************************************************************
ST_Database=''
src_connection= connectionSQLUnandPw(nexus_sql_servername,ST_Database,nexus_sql_username,nexus_sql_password)
print(f'Successfully connected to Database={ST_Database}')
ST_tables=['','','']
for tablename in ST_tables:
    query_Payment = f"""SELECT count(*) FROM {ST_Database}.dbo.{tablename} WHERE SystemModifiedDate >= DATEADD(day, {hour}, GETDATE())"""
    get_dataPresent(src_connection, query_Payment, tablename)

# *****************************************************************************************************************
UADM_Database=''
src_connection= connectionSQLUnandPw(nexus_sql_servername,UADM_Database,nexus_sql_username,nexus_sql_password)
print(f'Successfully connected to Database={UADM_Database}')
UADM_tablename = ''
query_UADM = f"""SELECT count(*) FROM {UADM_Database}.dbo.{UADM_tablename} WHERE DateModified >= DATEADD(hour, {hour}, GETDATE())"""
get_dataPresent(src_connection, query_UADM, UADM_tablename)

# ********************************************************************************************************************
Databasename=''
Schemaname=''
Role=''
Warehouse=''
src_connection= connect_Sf(snowflake_newusername,snowflake_newpassword,snowflake_newaccountname,Databasename,Schemaname,Role,Warehouse)
# print(f'Successfully connected to Database={Databasename}')
Snowflake_tablenames = ['','','','']
for tablename in Snowflake_tablenames:
    query_Snowflake = f"""SELECT count(*) FROM {Databasename}.{Schemaname}.{tablename} WHERE SYSTEMMODIFIEDDATE >= DATEADD('day', {hour}, CURRENT_TIMESTAMP())"""
    get_dataPresent(src_connection, query_Snowflake, tablename)

# ********************************************************************************************************************
Five9Databasename=''
Five9Schemaname=''
Five9Role=''
Five9Warehouse=''
Five9DBname=''
Five9Role1=''
Five9Warehouse1=''
src_connection= connect_Sf(snowflake_newusername,snowflake_newpassword,snowflake_newaccountname,Five9Databasename,Five9Schemaname,Five9Role,Five9Warehouse)
SFFive9_tablenames = ['','','','']
for tablename in SFFive9_tablenames:
    query_Snowflake = f"""SELECT count(*) FROM {Five9Databasename}.{Five9Schemaname}.{tablename} WHERE RECORDDATEANDHOUR >= TO_DATE(DATEADD('day', -2, CURRENT_TIMESTAMP()))"""
    get_dataPresent(src_connection, query_Snowflake, tablename)

src_connection= connect_Sf(snowflake_newusername,snowflake_newpassword,snowflake_newaccountname,Five9DBname,Five9Schemaname,Five9Role1,Five9Warehouse1)
SF_tablename='ACDQUEUEDETAILS'
query_Snowflake1 = f"""SELECT count(*) FROM {Five9DBname}.{Five9Schemaname}.{SF_tablename} WHERE RECORDDATEANDHOUR >= TO_DATE(DATEADD('day', -2, CURRENT_TIMESTAMP()))"""
get_dataPresent(src_connection, query_Snowflake1, SF_tablename)
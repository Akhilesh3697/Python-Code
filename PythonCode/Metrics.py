from utilities import *
from properties import *
from Query import *
import math
from datetime import date
from datetime import datetime

current_Date=date.today()
time=' 07:01:11.653'
# time=' 12:41:26.893'
current_Date_Time=f'{current_Date}{time}'
current_Month=datetime.now().month
start_date=datetime.now().replace(day=1).date()
Annual='DAY(GETDATE())'

def get_sql_server_data_count(src_connection,query):
    try:
       
        # print(src_connection)
 
        # Create a cursor object to execute SQL queries
        cursor = src_connection.cursor()
 
        # Execute the SQL query
        cursor.execute(query)
 
        # Fetch the result
        result = cursor.fetchone()
 
        # Get the count from the result
        count = result[0]
 
        # Close the cursor and connection
        cursor.close()
 
        # Return the count
        return count
 
    except pyodbc.Error as e:
        print(f"Error connecting to SQL Server: {e}")
        
def validate_counts(count1, count2):
    if count1 == count2:
        return f'source_count={count1} Data is matching with target_count={count2}'
    else:
        return f'source_count={count1} Data is not matching with target_count={count2}'

def get_roundup_value(number):
    # rounded_number = math.ceil(number * 100) / 100
    # return rounded_number
    rounded_number = round(number, 2)
    return rounded_number

def get_onenumber_roundup_value(value):
    return int(value + 0.5)

 # Establish a connection to the SQL Server
# src_connection= connectionSQL(Metrics_src_sql_servername,Metrics_src_sql_db)    
src_connection= connectionSQLUnandPw(sql_servername,sql_dbsub,sql_username,sql_password)       
# Members
MembersFS_count=get_sql_server_data_count(src_connection,Members_Query.format(current_Date,current_Date_Time))
# print(Source_count)
MembersMetrics_count=get_sql_server_data_count(src_connection,Metrics_Query.format('MEMBERS',current_Month))
# print(Target_count)
print(f'Member={validate_counts(MembersFS_count,MembersMetrics_count)}')

# MRRTotal
MRRTotalFS_count=MRRmonthlyFS_count+MRRannualFS_count
MRRTotalMetrics_count=get_sql_server_data_count(src_connection,Metrics_Query.format('MRR',current_Month))
print(f'MRR total={validate_counts(MRRTotalFS_count,MRRTotalMetrics_count)}')

# ReactivationMRRMonthly
RMRRmonthlyFS_count=get_sql_server_data_count(src_connection,RMmonthly_Query.format(start_date,current_Date,start_date,current_Date,current_Date_Time))
if RMRRmonthlyFS_count is None:
    RMRRmonthlyFS_count=0
RMRRmonthlyMetrics_count=get_sql_server_data_count(src_connection,MonthlyMetrics_Query.format('ReactivationMRR',current_Month))
print(f'ReactivationMRR monthly count={validate_counts(RMRRmonthlyFS_count,RMRRmonthlyMetrics_count)}')


#ARPUtotal
MembersTotal_count=MembersFS_count
ARPUtotalFS_count=get_roundup_value(MRRTotalFS_count/MembersTotal_count)
ARPUtotalMetrics_count=get_sql_server_data_count(src_connection,Metrics_Query.format('ARPU',current_Month))
print(f'ARPU total count={validate_counts(ARPUtotalFS_count,ARPUtotalMetrics_count)}')

# Sytem Modified Date
SystemModifiedDate_Mtrics=get_sql_server_data_count(src_connection,SystemModifiedDate_Query.format(current_Month))
print(f'SystemModifiedDate={validate_counts(current_Date,SystemModifiedDate_Mtrics)}')

src_connection.close()
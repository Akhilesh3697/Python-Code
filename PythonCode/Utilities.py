from sqlalchemy import create_engine
import snowflake.connector
import os
import configparser
from loguru import logger
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

script_dir = os.path.dirname(os.path.abspath(__file__))
# Specify the path to your config file relative to the script's directory
config_file_path = os.path.join(script_dir, 'config.ini')
# Read credentials from the config file
config = configparser.ConfigParser()
config.read(config_file_path)

# Function to connect MySQL DataBase
def connectMySQL(mysqlserver,myun,mypwd,DBName):
    try:
        a = f'mysql+pymysql://{myun}:{mypwd}@{mysqlserver}/{DBName}'
        
        # print(a)
        cnx = create_engine(a)   
        # print(f'\nConnecting to: {mysqlserver} ',end='')
        print(f'Successfully Connected to:{mysqlserver}')
        return cnx
    except Exception as err:
        print('\nError occurred while connecting: %s'%err) 
        
#Function to connect to Snowflake Database
def connect_Sf(userName,password,accountName,dbName,schema,role,warehouse):
    try :
        # print(userName,password,accountName,dbName,schema)
        # print(f'\nConnecting to: {dbName} \n',end='')
        connection = snowflake.connector.connect(user=userName,password=password,account=accountName,database=dbName,schema=schema,role=role,warehouse=warehouse)
        print(f'\n Successfully Connected to: {dbName} ',end='')
        return connection
    except Exception as err:
        print('\nError occurred while connecting: %s'%err)
        
#Function to connect SQL DataBase
def connectionSQL(server,databasename):
    odbc='{ODBC Driver 17 for SQL Server}'
    conn = pyodbc.connect(f'''Driver={odbc};
                          Server={server};
                          Database={databasename};
                          Trusted_Connection=yes;''')
    print(f'Connection got success for {server}')
    return conn

#Function to connect SQl Database using username and password.
def connectionSQLUnandPw(server,databasename,username,password):
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=' + server + ';'
        r'DATABASE=' + databasename + ';'
        r'UID=' + username + ';'
        r'PWD=' + password + ';'
    )
    conn = pyodbc.connect(conn_str)
    print(f'Connection got success for {server}')
    return conn

# # Function to connect Snowflake DataBase Using authentication
# def connectSnowflake_auth(sf_user,sf_account,sf_role,sf_warehouse,sf_database,sf_schema):
#     try:
#         print(f'\nConnecting to: {sf_database} \n',end='')
#         cnx = snowflake.connector.connect(user=sf_user,
#                                         account=sf_account, 
#                                         role=sf_role,
#                                         warehouse=sf_warehouse,
#                                         database=sf_database,       
#                                         schema =sf_schema,
#                                         authenticator="externalbrowser",
#                                         autocommit=True)    
#         print(f'\nConnected to: {sf_database} ',end='')
#         print('Successfully Connected to a Database.')
#         return cnx
#     except Exception as err:
#         print('\nError occurred while connecting: %s'%err) 

def get_logger():
    """
    The `get_logger` function reads a configuration file, and sets up a logger.
    """
    root_dir = os.path.dirname(os.path.abspath(__file__))
    logs_file_path = os.path.join(root_dir, 'logs')

    if not os.path.exists(logs_file_path):
        os.makedirs(logs_file_path)
    loguru_logger = logger
    loguru_logger.remove(0)
    loguru_logger.add(os.path.join(logs_file_path, "count_validation_{time:YYYY-MM-DD_HH}.log"), rotation="12:00",level="INFO",format="{time} - {level} - {message}",enqueue=True,colorize=True) 
    # logger.add(handler, level="ERROR")
    return loguru_logger

def send_notification(messages):
    """
    The `send_notification` function sends an email notification with the results of the data validation.
    """
    
    # Define the table headers and style
    table_headers = ["Mysql_Tablename", "SF_Tablename", "Mysql_count", "SF_count", "diff","count_match"]
    table_style = "border-collapse: collapse; width: 100%; border: 1px solid #000; font-family: Arial, sans-serif; font-size: 13px;\n"
    # Create the table header row
    table_html = f"<table style='{table_style}'>\n"
    table_html += "<tr>\n"
    for header in table_headers:
        table_html += f"<th style='border: 1px solid #000; padding: 4px;'>{header}</th>\n"
    table_html += "</tr>\n"

    for message in messages:
        status = message.get('count_match','')
        status_color = 'background-color: #00FF00' if status == 'TRUE' else 'background-color: #ff0000'
        table_html += "<tr>\n"
        for key in ["Mysql_Tablename", "SF_Tablename", "Mysql_count", "SF_count", "diff"]:
            table_html += f"<td style='border: 1px solid #000; padding: 4px;'>{message.get(key, '')}</td>\n"
        table_html += f"<td style='border: 1px solid #000; padding: 4px; {status_color};'>{status}</td>\n"
        table_html += "</tr>\n"
    table_html += "</table>"
    
    

    # Email configuration
    smtp_server = config.get('Email','host')
    smtp_port = int(config.get('Email','port'))
    sender_email = config.get('Email','sender_email')
    receiver_email = [email.strip() for email in config.get('Email','send_to').split(',')]

    # Create a message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)
    msg['Subject'] = f'Count Comaprison Report'
    msg.attach(MIMEText(table_html, 'HTML'))
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.ehlo()
        server.starttls()
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()
    except Exception as e:
        print(f'Error: {str(e)}')
        logger.error(f'Error: {str(e)}')
        
    print('Email sent successfully')        
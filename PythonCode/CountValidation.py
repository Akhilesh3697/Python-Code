from Utilities import *
import pandas as pd
import os
import configparser
from datetime import datetime
import warnings

def execute_data_comparison():

    warnings.filterwarnings('ignore')
    warnings.simplefilter('ignore')

    startCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
    print(f"Start of the Code Date and Time=",startCode)

    # logging configure
    logger = get_logger()
    logger.disable=True

    root_dir = os.path.dirname(os.path.abspath(__file__))

    metadata_file=f"{root_dir}\\ExcelFile.xlsx"
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Specify the path to your config file relative to the script's directory
    config_file_path = os.path.join(script_dir, 'config.ini')
    # Read credentials from the config file
    config = configparser.ConfigParser()
    config.read(config_file_path)

    emaildata = []
    
    server=config.get('MYSQLDataBase','Mysql_server')
    un=os.getenv('MYSQL_USER')
    # print(un)
    pw=os.getenv('MYSQL_PASSWORD')
    db=config.get('MYSQLDataBase','Mysql_dbname')
    src_connection= connectMySQL(server,un,pw,db)
    
    sf_un=os.getenv('SNOWFLAKE_USER')
    sf_pw=os.getenv('SNOWFLAKE_PASSWORD')
    accname=config.get('Snowflake','SF_accountname')
    sf_db=config.get('Snowflake','SF_dbname')
    schema=config.get('Snowflake','SF_schemaname')
    role=config.get('Snowflake','SF_role')
    warehouse=config.get('Snowflake','SF_warehouse')
    trg_connection = connect_Sf(sf_un,sf_pw,accname,sf_db,schema,role,warehouse)

    src_count=f"""{config.get('MYSQLQuery','Query')}"""
    tgt_count=f"""{config.get('SnowflakeQuery','NonPIIQuery')}"""

    SFPII_db=config.get('Snowflake','SFPII_dbname')
    PII_warehouse=config.get('Snowflake','SFPII_warehouse')
    PII_role=config.get('Snowflake','SFPII_role')
    trg_connection_PII = connect_Sf(sf_un,sf_pw,accname,SFPII_db,schema,PII_role,PII_warehouse)
    tgt_count_PII=f"""{config.get('SnowflakeQuery','PIIQuery')}"""

    # Taking the required tables from the Excel and converting to list
    data = pd.read_excel(metadata_file, sheet_name='ExcelSheetName')

    # Reading the source SQL_Tables values from Excel
    src_table_list = data['src'].tolist()

    # Reading the target SQL_Tables values from Excel
    tgt_table_list = data['tgt'].tolist()

    tablename = data['tablename'].tolist()

    # Exporting the data to new Excel Sheet
    i=0
    j=0
    for i in range (i, len(tgt_table_list)):
        for j in range (j, len(src_table_list)):
            j=j+1
            break
        if i==j:
            break

        # Reading the values from Source SQL
        df_src=pd.read_sql(con=src_connection,sql=src_count.replace('tbl',f'{src_table_list[i]}'))

        # Reading the values from Target SQL
        if '_PII' in tablename[i]:
            df_tgt=pd.read_sql(con=trg_connection_PII,sql=tgt_count_PII.replace('tbl',f'{tgt_table_list[i]}'))
        else:
            df_tgt=pd.read_sql(con=trg_connection,sql=tgt_count.replace('tbl',f'{tgt_table_list[i]}'))

        temp=''
        Diff=0
        if(df_tgt.iloc[0]['COUNT']==df_src.iloc[0]['COUNT']):
            temp='TRUE'
            Diff=(df_src.iloc[0]['COUNT']) - (df_tgt.iloc[0]['COUNT'])
        else:
            temp='FALSE'
            Diff=(df_src.iloc[0]['COUNT']) - (df_tgt.iloc[0]['COUNT'])
               
        data={
            'Mysql_Tablename':src_table_list[i],
            'SF_Tablename':tgt_table_list[i],
            'Mysql_count':df_src.iloc[0]['COUNT'],
            'SF_count':df_tgt.iloc[0]['COUNT'],
            'count_match':temp,
            'diff':Diff
        } 
        emaildata.append(data)
        logger.info('Mysql_Tablename={},SF_Tablename={},Mysql_count={},SF_count={},count_match={},diff={}'.format(src_table_list[i],tgt_table_list[i],df_src.iloc[0]['COUNT'],df_tgt.iloc[0]['COUNT'],temp,Diff))

    endCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
    print('End of Count Validation')
    send_notification(emaildata)
    print(f"End of the Code Date and Time=",endCode)
    # closing the connections
    src_connection.dispose()
    trg_connection.close()
if __name__ == "__main__":
    # Code to be executed when the script is run directly
    execute_data_comparison()
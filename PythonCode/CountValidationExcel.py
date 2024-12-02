from utilities import *
from properties import *
import pandas as pd
import os
from datetime import datetime
from datetime import date
from xlsxwriter.utility import xl_rowcol_to_cell
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

today = date.today()
startCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
print(f"Start of the Code Date and Time=",startCode)

home_dir=f""
output_dir=f"{home_dir}\\Count\\{str(today)}\\{datetime.now().strftime('%Y%m%d_%H%M')}"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

metadata_file=f"{home_dir}ExcelFile.xlsx"

# Path for exporting Excel file
out_file=f"count.xlsx"
output_file=os.path.join(output_dir,out_file)

# src_connection= connectMySQL(Moodle_Nexus_Mysql_server,MysqlNexus_username,MysqlNexus_password,MysqlNexus_dbname)
src_connection= connectMySQL(STG_Moodle_Nexus_Mysql_server,STG_MysqlNexus_username,STG_MysqlNexus_password,STG_MysqlNexus_dbname)
# trg_connection = connect_Sf(SFNexus_username,SFNexus_password,SFNexus_accountname,SFNexus_dbname,SFNexus_schemaname,SFNexus_role,SFNexus_warehouse)
trg_connection = connect_Sf(SFNexus_username,SFNexus_password,SFNexus_accountname,STG_SFNexus_dbname,SFNexus_schemaname,STG_SFNexus_role,STG_SFNexus_warehouse)

src_count=""" SELECT COUNT(*) as COUNT FROM DatabaseName.tbl """
tgt_count=""" SELECT COUNT(*) as COUNT FROM DatabaseName.SchemaName.tbl where ISDELETED='FALSE' """

SFNexusPII_dbname=""
SFNexusPII_warehouse=""
SFNexusPII_role=""
trg_connection_PII = connect_Sf(SFNexus_username,SFNexus_password,SFNexus_accountname,SFNexusPII_dbname,SFNexus_schemaname,SFNexusPII_role,SFNexusPII_warehouse)
tgt_count_PII=""" SELECT COUNT(*) as COUNT FROM DatabaseName.SchemaName.tbl where ISDELETED='FALSE' """

# Taking the required tables from the Excel and converting to list
data = pd.read_excel(metadata_file, sheet_name='ExcelSheetNamw')

# Reading the source SQL_Tables values from Excel
src_table_list = data['src'].tolist()

# Reading the target SQL_Tables values from Excel
tgt_table_list = data['tgt'].tolist()

tablename = data['tablename'].tolist()
    
# Exporting the data to new Excel Sheet
with pd.ExcelWriter(output_file) as out_file:
    i=0
    j=0
    df = pd.DataFrame(columns = ['Src_TableName_Mysql', 'Src_Count', 'Tgt_TableName_SF', 'Tgt_Count', 'Count_Match','Diff'])
    for i in range (i, len(tgt_table_list)):
        # print(tgt_table_list[i])
        for j in range (j, len(src_table_list)):
            # print(src_table_list[j])
            j=j+1
            break
        if i==j:
            break
        
        # Reading the values from Source SQL
        df_src=pd.read_sql(con=src_connection,sql=src_count.replace('tbl',f'{src_table_list[i]}'))
        # print(f'source count={df_src}')
        
        # df_tgt=pd.read_sql(con=trg_connection,sql=tgt_count.replace('tbl',f'{tgt_table_list[i]}'))

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
        
        my_list= [src_table_list[i],df_src.iloc[0]['COUNT'],tgt_table_list[i],df_tgt.iloc[0]['COUNT'],temp,Diff]
        df.loc[i]=my_list

        df.to_excel(out_file, 'count', index=False)

print(f'Output file {output_file} generated.')

endCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
print(f"End of the Code Date and Time=",endCode)
# closing the connections
src_connection.dispose()
trg_connection.close()

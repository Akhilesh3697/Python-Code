import os
from utilities import *
from properties import *
from datetime import datetime
from datetime import date
import pandas as pd
from xlsxwriter.utility import xl_rowcol_to_cell
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

today = date.today()
startCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
print(f"Start of the Code Date and Time=",startCode)

home_dir=f""
# "C:\\qe_automation_NASM\\"
output_dir=f"{home_dir}\\SchemaValidation\\{str(today)}\\{datetime.now().strftime('%Y%m%d_%H%M')}"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

metadata_file=f"{home_dir}ExcelFile.xlsx"

# src_connection= connectionSQL(src_sql_servername,src_sql_db)
# src_connection= connectionSQL(CS_src_sql_servername,CS_src_sql_db)
# src_connection= connectionSQL(src_sql_servername,src_sql_db)
src_connection= connectMySQL(mysql_server,mysql_username,mysql_password,mysql_dbname)

# trg_connection = connectionSQLUnandPw(trg_sql_servername,trg_sql_db,trg_sql_username,trg_sql_password)
# trg_connection= connectionSQL(trg_sql_servername,trg_sql_db)
trg_connection= connectionSQL(CS_tgt_sql_servername,CS_tgt_sql_db)

def generate_sql(tablename, src_tablename, tgt_tablename):
    try:
        # src_sql=f"""SELECT LOWER(TABLE_NAME) TABLE_NAME, LOWER(COLUMN_NAME) COLUMN_NAME,
        # DATA_TYPE, UPPER(IS_NULLABLE) IS_NULLABLE
        # FROM INFORMATION_SCHEMA.COLUMNS C WHERE TABLE_SCHEMA='dbo' and TABLE_NAME='{src_tablename}';"""
        src_sql=f"""SELECT LOWER(TABLE_NAME) TABLE_NAME, LOWER(COLUMN_NAME) COLUMN_NAME,
        DATA_TYPE, UPPER(IS_NULLABLE) IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS C WHERE TABLE_SCHEMA='' and TABLE_NAME='{src_tablename}';"""
        # src_sql=f"""SELECT LOWER(TABLE_NAME) TABLE_NAME, LOWER(COLUMN_NAME) COLUMN_NAME, 
		# 		CASE WHEN DATA_TYPE IN( 'int') THEN 'bigint'
		# 			 When DATA_TYPE IN( 'longtext') THEN 'text'
        #              When DATA_TYPE IN( 'bigint') THEN 'int'
        #              when DATA_TYPE IN( 'varchar') THEN 'nvarchar'
        #              when DATA_TYPE IN( 'tinyint') THEN 'int'
        #              when DATA_TYPE IN( 'timestamp') THEN 'datetime2'
        #         ELSE UPPER(DATA_TYPE)
        #         END AS DATATYPE, UPPER(IS_NULLABLE) IS_NULLABLE
        #         FROM INFORMATION_SCHEMA.COLUMNS C WHERE TABLE_SCHEMA = '' and  TABLE_NAME  = '{src_tablename}' ;"""

        tgt_sql=f"""SELECT LOWER(TABLE_NAME) TABLE_NAME, LOWER(COLUMN_NAME) COLUMN_NAME,
        DATA_TYPE, UPPER(IS_NULLABLE) IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS C WHERE TABLE_SCHEMA='' and TABLE_NAME='{tgt_tablename}';"""

        return src_sql,tgt_sql
    except Exception as e:
        print(f"Error in generate_sql for {tablename}:{e}")

def data_compare(tablename, src_connection, trg_connection, source_sql, target_sql, key_column):
    try:
        df_src=pd.read_sql(con=src_connection, sql=source_sql)
        # print(df_src)

        df_trg=pd.read_sql(con=trg_connection,sql=target_sql)
        # print(df_trg)

        merged_df=df_src.merge(df_trg, indicator=False, how='outer', left_index=False, right_index=False, \
                               sort=False, left_on=df_src['COLUMN_NAME'], right_on=df_trg['COLUMN_NAME'])
        # print(merged_df)
        merged_df.rename(columns={'TABLE_NAME_x':'TABLE_NAME_SRC', 'COLUMN_NAME_x':'COLUMN_NAME_SRC', 'DATA_TYPE_x':'DATATYPE_SRC', 'IS_NULLABLE_x':'IS_NULLABLE_SRC',
                                  'TABLE_NAME_y':'TABLE_NAME_TGT', 'COLUMN_NAME_y':'COLUMN_NAME_TGT', 'DATA_TYPE_y':'DATATYPE_TGT', 'IS_NULLABLE_y':'IS_NULLABLE_TGT'}, inplace=True)
        # print(merged_df)

        cols=merged_df.columns
        distinct_cols = ['TABLE_NAME','COLUMN_NAME','DATATYPE','IS_NULLABLE']

        for col in distinct_cols:
            if col != key_column:
                col_diff=f"{col}_diff"
                if col_diff == "TABLE_NAME_diff":
                    merged_df[col_diff]=merged_df[f"{col}_SRC"].str.replace('mdl_','') == merged_df[f"{col}_TGT"]
                else:
                    merged_df[col_diff]=merged_df[f"{col}_SRC"] == merged_df[f"{col}_TGT"]
        return merged_df
    except Exception as e:
        print(f"Error in data_compare for {tablename}={e}")

def formate_excel(tablename,df):
    try:
        file = f"{output_dir}//{tablename}.xlsx"
        writer = pd.ExcelWriter(file, engine='xlsxwriter')
        df.to_excel(writer, sheet_name=tablename, index=False)
        workbook = writer.book
        worksheet = writer.sheets[tablename]
        format_start_col_position = min([df.columns.get_loc(col) for col in df.columns if '_diff' in col])
        format_start_cell = xl_rowcol_to_cell(row=1,col=format_start_col_position)
        format_end_cell = xl_rowcol_to_cell(row=df.shape[0],col=df.shape[1]-1)

        cell_format_red = workbook.add_format({'bold': True, 'bg_color': 'red'})

        worksheet.conditional_format(f'{format_start_cell}:{format_end_cell}', {'type':'cell', 'criteria':'=', 'value':'FALSE', 'format':cell_format_red})

        writer.close()
        print(f"\nSchema Validation of {tablename} table is Done", end='')

    except Exception as e:
        print(f"Error in format_excel for {tablename}: {e}")


def wrapper_loop(metadata_file):
    df_metadata = pd.read_excel(metadata_file, sheet_name='ExcelSheetName')

    tablelist= df_metadata['tablename'].dropna().tolist()
    for tablename in tablelist:
        try:
            src_tablename=df_metadata['src'][df_metadata.tablename==tablename].values[0]
            tgt_tablename=df_metadata['tgt'][df_metadata.tablename==tablename].values[0]
            key_column=df_metadata['tgt_keycol'][df_metadata.tablename==tablename].values[0]

            source_sql, target_sql = generate_sql(tablename, src_tablename, tgt_tablename)

            df= data_compare(tablename, src_connection, trg_connection, source_sql, target_sql, key_column)
            formate_excel(tablename,df)

        except Exception as e:
            print(f"Error in wrapper_loop for {tablename}: {e}")
            continue
    
    endCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
    print(f"End of the Code Date and Time=",endCode)
    src_connection.dispose()
    trg_connection.dispose()
    # src_connection.close()
    # trg_connection.close()


if __name__ == '__main__':
    wrapper_loop(metadata_file)
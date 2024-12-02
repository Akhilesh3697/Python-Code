from utilities import *
from properties import *
import pandas as pd
import os
from datetime import datetime
from datetime import date
from xlsxwriter.utility import xl_rowcol_to_cell
import datacompy
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

today = date.today()
startCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
print(f"Start of the Code Date and Time=",startCode)

home_dir=f""
output_dir=f"{home_dir}\\DataValidation\\{str(today)}\\{datetime.now().strftime('%Y%m%d_%H%M')}"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

metadata=f"{home_dir}ExcelFile.xlsx"

src_connection= connectionSQL(src_sql_servername,src_sql_db)

trg_connection = connectionSQLUnandPw(trg_sql_servername,trg_sql_db,trg_sql_username,trg_sql_password)

def data_compare(tablename, src_connection, trg_connection, src_sql, tgt_sql, key_col):
    try:
        df_src = pd.read_sql(con=src_connection,sql=src_sql)

        df_tgt = pd.read_sql(con=trg_connection,sql=tgt_sql)
        
        # df_src['TIMESTAMPMILLISECONDS'] = df_src['TIMESTAMPMILLISECONDS'].dt.tz_localize(None)
        # df_tgt['TIMESTAMPMILLISECONDS'] = df_tgt['TIMESTAMPMILLISECONDS'].dt.tz_localize(None)

        output_file = f"{output_dir}{tablename}.xlsx"
        output_summaryfile = os.path.join(output_dir, f"{tablename}.txt")
        compare = datacompy.Compare(df_src.fillna('NULL'), df_tgt.fillna('NULL'), join_columns=key_col, df1_name='src', df2_name='tgt')
        compare.matches(ignore_extra_columns = True)

        unequal_rows = compare.df1_unq_rows
        df1_unequal_rows = pd.DataFrame(unequal_rows)
        unequal_rows_sql = compare.df2_unq_rows
        df2_unequal_rows = pd.DataFrame(unequal_rows_sql)
        all_match = compare.intersect_rows
        df_all_match = pd.DataFrame(all_match)
        all_mismatch = pd.DataFrame(compare.all_mismatch())

        out_file = f"{output_dir}\\{tablename}_SQL_vs_SQL.xlsx"
        with pd.ExcelWriter(out_file) as writer:
            date_columns = df1_unequal_rows.select_dtypes(include=['datetime64[ns, UTC]']).columns
            for date_column in date_columns:
                df1_unequal_rows[date_column] = df1_unequal_rows[date_column].dt.date
            df1_unequal_rows.to_excel(writer,'rows in Src_but_not_in_Tgt',index=False)
            
            date_columns = df2_unequal_rows.select_dtypes(include=['datetime64[ns, UTC]']).columns
            for date_column in date_columns:
                df2_unequal_rows[date_column] = df2_unequal_rows[date_column].dt.date
            df2_unequal_rows.to_excel(writer,'rows_in_Tgt_but_not_in_Src',index=False)

            date_columns = all_mismatch.select_dtypes(include=['datetime64[ns, UTC]']).columns
            for date_column in date_columns:
                all_mismatch[date_column] = all_mismatch[date_column].dt.date
            all_mismatch.to_excel(writer,'all_mismatch',index=False)

            date_columns = df_all_match.select_dtypes(include=['datetime64[ns, UTC]']).columns
            for date_column in date_columns:
                df_all_match[date_column] = df_all_match[date_column].dt.date
            df_all_match.to_excel(writer,'rows in Src_and_Tgt',index=False)

        with open(output_summaryfile, 'w') as f:
            f.write(compare.report())

        df_mismatch = compare.all_mismatch()
        cols = df_mismatch.columns
        distinct_cols = set([col.replace('_df1','').replace('_df2','') for col in cols])

        for col in distinct_cols:
            if col != key_col.lower():
                col_diff = f"{col}_diff"
                df_mismatch[col_diff] = df_mismatch[f"{col}_df1"] == df_mismatch[f"{col}_df2"]
            else:
                continue

        return df_mismatch
    
    except Exception as e:
        print(f"Error in data_compare for {tablename}: {e}")

def format_excel(tablename, df):
    try:
        file = f"{output_dir}//{tablename}.xlsx"
        writer = pd.ExcelWriter(file, engine='xlsxwriter')
        df.to_excel(writer, sheet_name=tablename, index=False)
        workbook = writer.book
        worksheet = writer.sheets[tablename]
        format_start_col_position = min([df.columns.get_loc(col) for col in df.columns if '_diff' in col])
        format_start_cell = xl_rowcol_to_cell(row=1,col=format_start_col_position)
        format_end_cell = xl_rowcol_to_cell(row=df.shape[0],col=df.shape[1]-1)

        # print(format_start_cell,format_end_cell)

        cell_format_red = workbook.add_format({'bold': True, 'bg_color': 'red'})

        worksheet.conditional_format(f'{format_start_cell}:{format_end_cell}', {'type':'cell', 'criteria':'=', 'value':'FALSE', 'format':cell_format_red})

        writer.close()
        print(f"\nData Validation of {tablename} table is Done", end='')

    except Exception as e:
        print(f"Error in format_excel for {tablename}: {e}")

def wrapper_loop(metadata_file):
    df_metadata = pd.read_excel(metadata_file, sheet_name='ExcelSheetName')

    tablelist = df_metadata['tablename'].dropna().to_list()
    for tablename in tablelist:
        try:
            src_tablename = df_metadata['src'][df_metadata.tablename == tablename].values[0]
            tgt_tablename = df_metadata['tgt'][df_metadata.tablename == tablename].values[0]
            key_col = df_metadata['tgt_keycol'][df_metadata.tablename == tablename].values[0]

            if df_metadata['src_filter'][df_metadata.tablename == tablename].isnull().all():
                src_filter=''
            else:
                src_filter=df_metadata['src_filter'][df_metadata.tablename == tablename].values[0]
            
            if df_metadata['tgt_filter'][df_metadata.tablename == tablename].isnull().all():
                tgt_filter=''
            else:
                tgt_filter=df_metadata['tgt_filter'][df_metadata.tablename == tablename].values[0]
            
            src_sql = f""" SELECT * FROM DatabaseName.dbo.{src_tablename} order by {key_col} DESC LIMIT 25;"""

            tgt_sql = f"""SELECT top 25 * FROM DatabaseName.dbo.{tgt_tablename} order by {key_col} DESC;"""

            df = data_compare(tablename, src_connection, trg_connection, src_sql, tgt_sql, key_col)
            format_excel(tablename, df)
        
        except Exception as e:
            print(f"Error in wrapper_loop for {tablename}: {e}")
            continue
    
    endCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
    print(f"End of the Code Date and Time=",endCode)
    src_connection.dispose()
    trg_connection.dispose()
    # src_connection.close()
    # trg_connection.close()

if __name__=="__main__":
    wrapper_loop(metadata)



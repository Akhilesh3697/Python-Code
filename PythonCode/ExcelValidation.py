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
output_dir=f"{home_dir}\\ExcelValidation\\{str(today)}\\{datetime.now().strftime('%Y%m%d_%H%M')}"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

src_file=f""

tgt_file=f""

key_col=f""

def data_compare(src_file, tgt_file, key_col):
    try:
        df_src = pd.read_excel(src_file)
        
        src_count = len(df_src.index)
        df_tgt = pd.read_excel(tgt_file)
        tgt_count = len(df_tgt.index)
        df_count = pd.DataFrame(columns = ['Src_Count', 'Tgt_Count', 'Count_Match'])
        temp = src_count == tgt_count
        my_list= [src_count,tgt_count,temp]
        df_count.loc[0]=my_list


        output_file = f"{output_dir}ExcelValidation.xlsx"
        output_summaryfile = os.path.join(output_dir, f"ExcelValidation.txt")
        # print(df_src)
        # print(df_tgt)
        compare = datacompy.Compare(df_src.fillna('NULL'), df_tgt.fillna('NULL'), join_columns=key_col.split(","), df1_name='src', df2_name='tgt')
        # print(compare)
        compare.matches(ignore_extra_columns = True)

        unequal_rows = compare.df1_unq_rows
        df1_unequal_rows = pd.DataFrame(unequal_rows)
        unequal_rows_sql = compare.df2_unq_rows
        df2_unequal_rows = pd.DataFrame(unequal_rows_sql)
        all_match = compare.intersect_rows
        df_all_match = pd.DataFrame(all_match)
        all_mismatch = pd.DataFrame(compare.all_mismatch())

        out_file = f"{output_dir}\\ExcelValidationDimPaymentPlanMapping.xlsx"
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
            df_all_match.to_excel(writer,'all_matching',index=False)

        with open(output_summaryfile, 'w') as f:
            f.write(compare.report())

        df_mismatch = compare.all_mismatch()
        cols = df_mismatch.columns
        distinct_cols = set([col.replace('_df1','').replace('_df2','') for col in cols])

        for col in distinct_cols:
            # if col != key_col.lower():
            if col not in key_col.lower():
                col_diff = f"{col}_diff"
                df_mismatch[col_diff] = df_mismatch[f"{col}_df1"] == df_mismatch[f"{col}_df2"]
            else:
                continue

        return df_mismatch,df1_unequal_rows,df2_unequal_rows,df_count
    
    except Exception as e:
        print(f"Error in data_compare for Excel sheet Validation: {e}")

def format_excel(df_mismatch,df1_unequal_rows,df2_unequal_rows,df_count):
    try:
        file = f"{output_dir}//ExcelValidation.xlsx"
        writer = pd.ExcelWriter(file, engine='xlsxwriter')

        df1_unequal_rows.to_excel(writer, sheet_name="rows in Src_but_not_in_Tgt", index=False)
        df2_unequal_rows.to_excel(writer, sheet_name="rows in Tgt_but_not_in_Src", index=False)
        df_count.to_excel(writer, sheet_name="Count", index=False)
        df_mismatch.to_excel(writer, sheet_name="ExcelValidation", index=False)
        # print(df_mismatch)
        workbook = writer.book
        worksheet = writer.sheets["ExcelValidation"]
        format_start_col_position = min([df_mismatch.columns.get_loc(col) for col in df_mismatch.columns if '_diff' in col])
        format_start_cell = xl_rowcol_to_cell(row=1,col=format_start_col_position)
        format_end_cell = xl_rowcol_to_cell(row=df_mismatch.shape[0],col=df_mismatch.shape[1]-1)

        # print(format_start_cell,format_end_cell)

        cell_format_red = workbook.add_format({'bold': True, 'bg_color': 'red'})

        worksheet.conditional_format(f'{format_start_cell}:{format_end_cell}', {'type':'cell', 'criteria':'=', 'value':'FALSE', 'format':cell_format_red})

        writer.close()
        print(f"Data Validation of Excel sheet is Done", end='')

    except Exception as e:
        print(f"Error in format_excel for Excel sheet validation: {e}")
    endCode = datetime.now().strftime('%Y-%m-%d_%H:%M')
    print(f"End of the Code Date and Time=",endCode)

def wrapper_loop():
        try:
            df_mismatch,df1_unequal_rows,df2_unequal_rows,df_count = data_compare(src_file,tgt_file,key_col)
            format_excel(df_mismatch,df1_unequal_rows,df2_unequal_rows,df_count)
        
        except Exception as e:
            print(f"Error in wrapper_loop for Excel Validation: {e}")


if __name__=="__main__":
    wrapper_loop()



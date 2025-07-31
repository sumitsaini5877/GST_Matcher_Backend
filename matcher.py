import pandas as pd

# #------------------Create Unique Key Function--------------------
# def create_unique_key(df, gstin_col, invoice_col, date_col):
#     try:
#         unique_key = (
#             df[gstin_col].astype(str).str.upper().str.strip() + '-' +
#             df[invoice_col].astype(str).str.upper().str.strip() + '-' +
#             pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%d%m%y')
#         )
#         return unique_key
#     except Exception as e:
#         raise ValueError(f"Error in creating unique key: {e}")

# #------------------Find Header Row Function----------------------
# def find_header_row(file_path, keyword='GSTIN'):
#     try:
#         df = pd.read_excel(file_path, header=None)
#         for idx, row in df.iterrows():
#             if row.astype(str).str.contains(keyword, case=False, na=False).any():
#                 return idx
#         return None
#     except Exception as e:
#         raise ValueError(f"Failed to find header row: {e}")

# #------------------Clean Headers Function----------------------
# def clean_headers(df):
#     try:
#         cleaned_columns = []
#         for col_tuple in df.columns:
#             main_col = str(col_tuple[0]).strip()
#             sub_col = str(col_tuple[1]).strip()
#             if "unnamed" in sub_col.lower() or sub_col == '':
#                 cleaned_columns.append(main_col)
#             else:
#                 cleaned_columns.append(f"{main_col} {sub_col}".strip())
#         return [col.strip().replace('  ', ' ') for col in cleaned_columns]
#     except Exception as e:
#         raise ValueError(f"Failed to clean headers: {e}")

# #------------------Process Purchase File-----------------------
# def process_purchase_file(file_path):
#     header_row = find_header_row(file_path)
#     if header_row is None:
#         raise ValueError("Header row with 'GSTIN' keyword not found in the purchase file")
#     try:
#         df = pd.read_excel(file_path, header=[header_row + 1, header_row + 2])
#         df.columns = clean_headers(df)
#         return df
#     except Exception as e:
#         raise ValueError(f"Error processing purchase file: {e}")

# #------------------Main Match Function----------------------
# def match_files(gst_df, purchase_file):
#     try:
#         purchase_df = process_purchase_file(purchase_file)

#         gst_df['unique_key'] = create_unique_key(gst_df, 'GSTIN/UIN', 'Voucher No.', 'Date')
#         purchase_df['unique_key'] = create_unique_key(purchase_df, 'GSTIN of supplier', 'Invoice Details Invoice number', 'Invoice Details Invoice Date')

#         merged_df = pd.merge(
#             purchase_df, gst_df,
#             how='outer',
#             on='unique_key',
#             suffixes=('_purchase', '_gst'),
#             indicator=True
#         )

#         matched = merged_df[merged_df['_merge'] == 'both']
#         only_in_gst = merged_df[merged_df['_merge'] == 'right_only']
#         only_in_purchase = merged_df[merged_df['_merge'] == 'left_only']

#         return {
#             "matched_count": len(matched),
#             "only_in_gst_count": len(only_in_gst),
#             "only_in_purchase_count": len(only_in_purchase),
#             "matched": matched.head(100).to_dict(orient="records"),
#             "only_in_gst": only_in_gst.head(100).to_dict(orient="records"),
#             "only_in_purchase": only_in_purchase.head(100).to_dict(orient="records")
#         }
#     except Exception as e:
#         raise ValueError(f"Matching process failed: {e}")









#---------------------Create Unique Key Function--------------------
def create_unique_key(df,gstin_col,invoice_col,date_col):
    unique_key = (df[gstin_col].str.upper().str.strip()+'-'+\
                 df[invoice_col].str.upper().str.strip()+'-'+\
                 pd.to_datetime(df[date_col],errors='coerce').dt.strftime('%d%m%y'))
    return unique_key
#------------------Create Find Header Row Function-------------------
def find_header_row(file_path,keyword='GSTIN'):
    df = pd.read_excel(file_path)
    for idx , rows in df.iterrows():
        if rows.astype(str).str.contains(keyword, case=False, na=False).any():
            return idx
    return None
#----------------- Create Clean Header Function---------------------
def clean_headers(df):
    cleaned_columns = []
    for col_tuple in df.columns:
        main_col = str(col_tuple[0]).strip()
        sub_col = str(col_tuple[1]).strip()

        if "unnamed" in sub_col.lower() or sub_col.strip()=='':
            cleaned_columns.append(main_col)
        else:
            cleaned_columns.append(f"{main_col} {sub_col}")

    return [col.strip().replace('  ',' ') for col in cleaned_columns]

#-----------------Create Process Purchase File Function--------------------
def process_Purchase_file(file_path):
   header_row = find_header_row(file_path)
   if header_row is None:
       raise ValueError("Header row with GSTIN keyword not found")
   df = pd.read_excel(file_path,header=[header_row+1,header_row+2])
   df.columns = clean_headers(df)
   return df

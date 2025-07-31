from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
from io import BytesIO
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gst-matcher.vercel.app/"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/hello")
def read_root():
    return JSONResponse({"message": "Hello from FastAPI"})


# --------------------- Utility Functions ------------------------

def create_unique_key(df, gstin_col, invoice_col, date_col):
    unique_key = (df[gstin_col].str.upper().str.strip() + '-' +
                  df[invoice_col].str.upper().str.strip() + '-' +
                  pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%d%m%y'))
    return unique_key

def clean_headers(df):
    cleaned_columns = []
    for col_tuple in df.columns:
        main_col = str(col_tuple[0]).strip()
        sub_col = str(col_tuple[1]).strip()
        if "unnamed" in sub_col.lower() or sub_col.strip() == '':
            cleaned_columns.append(main_col)
        else:
            cleaned_columns.append(f"{main_col} {sub_col}")
    return [col.strip().replace('  ', ' ') for col in cleaned_columns]

def find_header_row(df, keyword='GSTIN'):
    for idx, row in df.iterrows():
        if row.astype(str).str.contains(keyword, case=False, na=False).any():
            return idx
    return None

def process_purchase_file(file_bytes):
    df_raw = pd.read_excel(BytesIO(file_bytes))
    header_row = find_header_row(df_raw)
    if header_row is None:
        raise ValueError("Header row with GSTIN keyword not found")
    df = pd.read_excel(BytesIO(file_bytes), header=[header_row + 1, header_row + 2])
    df.columns = clean_headers(df)
    return df


# --------------------- Main API ------------------------


@app.get("/")
def root():
    return {"message": "GST Matcher Backend is running. Use /match-files/ to access the matching route."}

@app.post("/match-files/")
async def match_files(gst_file: UploadFile = File(...), purchase_file: UploadFile = File(...)):
    try:
        # Read files
        gst_bytes = await gst_file.read()
        purchase_bytes = await purchase_file.read()

        # Load DataFrames
        gst_df = pd.read_excel(BytesIO(gst_bytes), skiprows=7).iloc[:-1]
        purchase_df = process_purchase_file(purchase_bytes).iloc[:-1]

        # Generate unique keys
        gst_df['unique_key'] = create_unique_key(gst_df, 'GSTIN/UIN', 'Voucher No.', 'Date')
        purchase_df['unique_key'] = create_unique_key(
            purchase_df, 'GSTIN of supplier', 'Invoice Details Invoice number', 'Invoice Details Invoice Date'
        )

        # Full outer merge
        merged_outer_df = pd.merge(
            purchase_df, gst_df, how='outer', on='unique_key',
            suffixes=('_purchase', '_gst'), indicator=True
        )

        # Separate matching and non-matching records
        matching = merged_outer_df[merged_outer_df['_merge'] == 'both'].copy()
        non_matching = merged_outer_df[merged_outer_df['_merge'] != 'both'].copy()

        # Replace non-serializable values and convert datetimes to string
        def clean_dataframe(df):
            df = df.replace({pd.NA: "", pd.NaT: "", float("nan"): "", None: ""})
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_categorical_dtype(df[col]):
                    df[col] = df[col].astype(str)
            return df

        matching_cleaned = clean_dataframe(matching)
        non_matching_cleaned = clean_dataframe(non_matching)

        # Prepare response
        response_data = {
            "status": "success",
            "total_records": len(matching_cleaned) + len(non_matching_cleaned),
            "matching_records": len(matching_cleaned),
            "non_matching_records": len(non_matching_cleaned),
            "match_data": matching_cleaned.to_dict(orient="records"),
            "non_match_data": non_matching_cleaned.to_dict(orient="records"),
        }

        return JSONResponse(response_data)

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

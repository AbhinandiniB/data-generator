#This file contains all the API endpoint description

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import xml.etree.ElementTree as ET
from config.configs import GenerateRequest
from processing.generate_csv import generate_csv_data
from processing.generate_json import generate_json_data
from processing.generate_xml import generate_xml_data, to_nested_xml
from processing.generate_sql import generate_sql_data, generate_insert_sql
from io import BytesIO, StringIO
import json
from schema.json_schema import parse_json_schema
from schema.xml_parse import parse_xml_element
from schema.sql_parse import parse_create_table_raw
from schema.flatten_payload import reconstruct_payload

app = FastAPI()

# Enable CORS for frontend-backend on different ports
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Streaming Data Endpoint ----
@app.post("/stream-data/generate")
def stream_data(payload: GenerateRequest):
    file_format = payload.config.format.lower()
    if file_format == "csv":
        media_type="text/csv"
        buffer = BytesIO()
        df = generate_csv_data(payload)
        df.to_csv(buffer, index=False, sep=payload.config.delimiter)
        buffer.seek(0)
    elif file_format == "tsv":
        media_type="text/csv"
        buffer = BytesIO()  
        df = generate_csv_data(payload)
        df.to_csv(buffer, index=False, sep='\t')
        buffer.seek(0)
    elif file_format == 'json':
        media_type = "application/json"
        reconstructed_payload = reconstruct_payload(payload)
        data = generate_json_data(reconstructed_payload)

        indent = reconstructed_payload.config.indent or None
        json_lines = reconstructed_payload.config.jsonLines

        # data = generate_json_data(payload)
        # indent = payload.config.indent or None
        # json_lines = payload.config.jsonLines
        if json_lines:
            # Convert each record (dict) to a JSON line
            json_str = "\n".join(json.dumps(record, default=str) for record in data)
        else:
            # Pretty print full JSON array
            json_str = json.dumps(data, indent=indent, default=str)
        buffer = StringIO(json_str) 
        buffer.seek(0)   
    # elif file_format == 'sql':
        # media_type = "text/plain"
        # buffer = StringIO()
        # data = generate_sql_data(payload)
        # sql_str = generate_insert_sql(data)
        # # buffer = StringIO(sql_str) 
        # # buffer.seek(0)
        # buffer.write(sql_str)
        # buffer.seek(0)
    elif file_format == 'sql':
        media_type = "text/plain"
        buffer = StringIO()
        df = generate_sql_data(payload)
        
        sql_str = ""
        # if payload.config.generateCreateTable:
        #     create_stmt = generate_create_table_sql(df, table_name="mock_data")
        #     sql_str += create_stmt + "\n"
        
        insert_stmt = generate_insert_sql(df, table_name=payload.config.targetTable)
        sql_str += insert_stmt

        buffer.write(sql_str)
        buffer.seek(0)
    elif file_format == 'excel':
        buffer = BytesIO()
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        df = generate_csv_data(payload)
        file_extension = ".xlsx"
        df.to_excel(buffer, index=False, sheet_name=payload.config.sheetName)
        buffer.seek(0)
        return StreamingResponse(buffer, media_type=media_type,
                                 headers={"Content-Disposition": f"attachment; filename=mock_data{file_extension}"})
    elif file_format == 'xml':
        # df.to_xml(buffer, index=False)
        media_type = "application/xml"
        file_extension = ".xml"
        reconstructed_payload = reconstruct_payload(payload)
        xml_str = generate_xml_data(reconstructed_payload) # This should return a string, not bytes
        buffer = StringIO(xml_str)
        buffer.seek(0)
    elif file_format == 'parquet':
        buffer = BytesIO()
        media_type = "application/octet-stream"
        # file_extension = ".parquet"
        df = generate_csv_data(payload)
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
    else:
        return {"error": f"Unsupported format: {file_format}"}
    
    return StreamingResponse(buffer, media_type=media_type,
                                 headers={"Content-Disposition": f"attachment; filename=mock_data.{file_format}"})

@app.post("/parse-json/")
async def parse_json(file: UploadFile = File(...)):
    content = await file.read()
    data = json.loads(content)

    fields = []
    if isinstance(data, list):
        # Assume JSON array of objects
        first = data[0]
        fields = parse_json_schema(first)["items"]
    elif isinstance(data, dict):
        fields = parse_json_schema(data)["items"]

    return {"field": fields}

@app.post("/parse-xml/")
async def parse_xml(file: UploadFile = File(...)):
    content = await file.read()
    root = ET.fromstring(content)

    schema = parse_xml_element(root, field_name=root.tag)

    return {"field": [schema]}  

@app.post("/parse-sql/")
async def parse_sql(file: UploadFile = File(...)):
    content = await file.read()
    content = content.decode("utf-8")
    # print(content)
    # root = ET.fromstring(content)
    schema = parse_create_table_raw(content)
    print(schema)
    return {"field": [schema]}  


# from database.db import connect_to_db, get_full_schema, generate_data, write_to_db
# from database.db_configs import DBConfig, GenerationRequest
# import traceback
# from sqlalchemy.exc import SQLAlchemyError

# # ---------- API Route for table generation----------
# @app.post("/generate-from-table/")
# def generate_from_existing_table(req: GenerationRequest):
#     try:
#         engine = connect_to_db(**req.db_config.dict())
#         schema = get_full_schema(engine, req.source_table)
#         df = generate_data(schema, req.row_count)
#         write_to_db(df, req.destination_table, schema, engine)
#         return {
#             "status": "success",
#             "rows_generated": req.row_count,
#             "destination_table": req.destination_table
#         }
#     except SQLAlchemyError as db_err:
#         print(traceback.format_exc())
#         raise HTTPException(status_code=500, detail=f"Database error: {str(db_err)}")
#     except Exception as e:
#         print(traceback.format_exc())
#         raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# ---- Generate & Return Download URL ----
# # Temporary storage
# TEMP_DIR = "./temp_data"
# os.makedirs(TEMP_DIR, exist_ok=True)

# @app.post("/generate-file/")
# def generate_file(payload: GenerateRequest):
#     df = generate_csv_data(payload)

#     ext = payload.config.format.lower()
#     filename = f"{uuid.uuid4()}.{ext}"
#     file_path = os.path.join(TEMP_DIR, filename)

#     if ext == "csv":
#         df.to_csv(file_path, index=False, sep=",")
#     elif ext == "json":
#         df.to_json(file_path, orient="records", indent=2)
#     elif ext == "tsv":
#         df.to_csv(file_path, sep="\t", index=False)
#     elif ext == "xlsx":
#         df.to_excel(file_path, index=False)
#     elif ext == "parquet":
#         df.to_parquet(file_path, index=False)
#     else:
#         return {"error": f"Unsupported format: {ext}"}

#     return {"download_url": f"/download-file/{filename}"}

# # ---- Download Endpoint ----
# @app.get("/download-file/{filename}") 
# def download_file(filename: str):
#     file_path = os.path.join(TEMP_DIR, filename)
#     return FileResponse(path=file_path, filename=filename)
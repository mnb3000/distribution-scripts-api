import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, HTTPException
from starlette.responses import StreamingResponse

from app.report_split import split_csv
from app.report_sum import report_sum

app = FastAPI()
log_level = os.getenv("LOGLEVEL", default="INFO")
logging.basicConfig(level=os.getenv("LOGLEVEL", default="INFO"))
logger = logging.getLogger(__name__)
load_dotenv()


@app.post("/split", response_class=StreamingResponse)
def split_route(file: UploadFile = File(...), prefix: str = "report", artist_column: str = "Artist", encoding: str = "utf-8"):
    if not file.filename:
        raise HTTPException(status_code=422, detail="File should not be empty")
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=422, detail="File should be csv")
    logger.info(f"Received split request with filename: {file.filename}, prefix: {prefix}, artist_column: {artist_column}, encoding: {encoding}")
    zip = split_csv(file.file, prefix, artist_column, encoding)
    response = StreamingResponse(
        iter([zip.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename={prefix}.zip",
                 "Content-Length": str(zip.getbuffer().nbytes)}
    )
    return response


@app.post("/sum", response_model=dict)
def sum_route(
        file: UploadFile = File(...),
        artist_column: str = "Artist",
        net_revenue_column: str = "Net Revenue in USD",
        encoding: str = "utf-8"
):
    if not file.filename:
        raise HTTPException(status_code=422, detail="File should not be empty")
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=422, detail="File should be csv")
    return report_sum(file.file, artist_column, net_revenue_column, encoding)

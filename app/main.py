import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse

from app.report_split import split_csv
from app.report_sum import report_sum

app = FastAPI()

origins = [
    "https://distro.myk.digital",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

log_level = os.getenv("LOGLEVEL", default="INFO")
logging.basicConfig(level=os.getenv("LOGLEVEL", default="INFO"))
logger = logging.getLogger(__name__)
load_dotenv()


@app.post("/split", response_class=StreamingResponse)
def split_route(
        file: UploadFile = File(...),
        prefix: str = Body(default="report"),
        artist_column: str = Body(default="Artist"),
        encoding: str = Body(default="utf-8")
):
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
        artist_column: str = Body(default="Artist"),
        net_revenue_column: str = Body(default="Net Revenue in USD"),
        encoding: str = Body(default="utf-8")
):
    if not file.filename:
        raise HTTPException(status_code=422, detail="File should not be empty")
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=422, detail="File should be csv")
    logger.info(f"Received sum request with filename: {file.filename}, artist_column: {artist_column}, "
                f"net_revenue_column: {net_revenue_column}, encoding: {encoding}")
    return report_sum(file.file, artist_column, net_revenue_column, encoding)

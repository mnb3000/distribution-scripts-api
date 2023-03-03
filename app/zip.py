import io
import logging
import os
import zipfile

logger = logging.getLogger(__name__)


def zip_folder(path: str):
    logger.info(f"Zipping ${path}")
    zip_bytes_io = io.BytesIO()
    with zipfile.ZipFile(zip_bytes_io, 'w', zipfile.ZIP_DEFLATED) as zipped:
        for dirname, subdirs, files in os.walk(path):
            for filename in files:
                logger.debug(f"Adding ${filename} to zip")
                zipped.write(os.path.join(dirname, filename), filename)
                logger.debug(f"Added ${filename} to zip")
        zipped.close()
    return zip_bytes_io

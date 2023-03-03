import logging
import shutil
import tempfile

import pandas as pd
import os

from app.zip import zip_folder

logger = logging.getLogger(__name__)


def split_csv(file, prefix="report", artist_column="Artist", encoding="utf-8"):
    df = pd.read_csv(file, encoding=encoding)

    folder_path = tempfile.mkdtemp(prefix=f"{prefix}_")
    logger.debug(f"Created {folder_path} folder")

    logger.info(f"Splitting {prefix}")
    artists = df[artist_column].unique()
    for artist in artists:
        filename = f"{prefix}_{artist}.csv"
        filepath = os.path.join(folder_path, filename)
        filtered = df[df[artist_column] == artist]
        filtered.to_csv(filepath)
        logger.debug(f"Saved {folder_path}/{filename}")
    zip = zip_folder(folder_path)
    logger.debug(f"Removing ${folder_path}")
    shutil.rmtree(folder_path)
    logger.debug(f"Removed ${folder_path}")
    return zip

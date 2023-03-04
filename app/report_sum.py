import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def report_sum(file, artist_column="Artist", net_revenue_column="Net Revenue in USD", encoding="utf-8"):
    df = pd.read_csv(file, encoding=encoding)
    sums = df.groupby(artist_column).agg({net_revenue_column: 'sum'})
    jsonData = sums.to_json(orient='table')
    parsed = json.loads(jsonData)
    return parsed

import pandas as pd
import psycopg2

from interface import implements

from zipline.lib.adjusted_array import AdjustedArray

from .base import PipelineLoader
from .utils import shift_dates

class PSQLFundamentalsLoader(implements(PipelineLoader)):

    def __init__(self, db_path):
        self._conn = psycopg2.connect(db_path)
        self._col_cache = {}
    
    def _load_from_sql(self, column, domain):
        df = pd.read_sql(
            f"SELECT date, id, value FROM fundamentals WHERE name = '{column.name}' ORDER BY date, id",
            self._conn,
            index_col=['date','id'],
            parse_dates={'date': {'utc': True}})
        
        df['value'] = df['value'].astype(column.dtype)

        return df.unstack().ffill()
        
    def _load_column(self, column, dates, sids, domain):
        try:
            content = self._col_cache[column.name]
        except KeyError:
            content = self._col_cache[column.name] = self._load_from_sql(column, domain)

        content = content.reindex(dates, method='ffill')
        content.columns = content.columns.droplevel(0)
        
        return content.stack().reindex(
                      pd.MultiIndex.from_product([dates, sids])
                  ).unstack()[sids].to_numpy()

    def load_adjusted_array(self, domain, columns, dates, sids, mask):
        # load_adjusted_array is called with dates on which the user's algo
        # will be shown data, which means we need to return the data that would
        # be known at the **start** of each date. We assume that the latest
        # data known on day N is the data from day (N - 1), so we shift all
        # query dates back by a trading session.
        sessions = domain.all_sessions()
        shifted_dates = shift_dates(sessions, dates[0], dates[-1], shift=1)

        out = {}
        
        for column in columns:
            result = self._load_column(column, shifted_dates, sids, domain) 
            out[column] = AdjustedArray(
                result.astype(column.dtype),
                {}, # fundamental data has no adjustments
                column.missing_value,
            )

        return out

    @property
    def currency_aware(self):
        return False   



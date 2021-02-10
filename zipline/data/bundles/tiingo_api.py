"""
Module for building a complete dataset from tiingo
"""
import os
import sys

from logbook import Logger, StreamHandler
from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta, NaT
from trading_calendars import register_calendar_alias

from zipline.utils.cli import maybe_show_progress
from zipline.errors import SymbolNotFound, SidsNotFound
from zipline.data.bundles.universe import get_sp500, get_sp100, get_nasdaq100
from zipline.data.bundles.core import register
from zipline.data import bundles as bundles_module


from datetime import date, timedelta
import pandas as pd

from trading_calendars import TradingCalendar
import trading_calendars

from tiingo import TiingoClient
from tiingo.restclient import RestClientError

import time

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)

CLIENT = None

def init_client():
    global CLIENT
    CLIENT = TiingoClient({'session': True})

@register("tiingo")
def tiingo_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir,
                  fundamentals_writer=None):
    """
    Build a zipline data bundle from the tiing-api
    """
    init_client()

    assets = get_sp500()
    #assets = get_nasdaq100()

    #assets = ['AAPL']
    assets = assets + ['SPY']
    #assets = ['AAPL', 'TSLA', 'GOOGL', 'SPY']
    #assets = 'ALL'

    metadata, assets_to_sids = tiingo_metadata(assets, asset_db_writer.asset_finder)

    print('Writing assets...')
    asset_db_writer.write(equities=metadata)
    print('Done!')

    divs_splits = {'divs': DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'splits': DataFrame(columns=['sid', 'ratio',
                                                'effective_date'])}

    symbols = metadata['symbol'].values
    sids_written = []

    daily_bar_writer.write(
        _pricing_iter(symbols, divs_splits, show_progress,
                      metadata, sids_written, assets_to_sids = assets_to_sids),
    )

    #metadata = metadata[metadata.index.isin(sids_written)]

    divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
    divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)

    print('Writing adjustments...')
    adjustment_writer.write(splits=divs_splits['splits'],
                            dividends=divs_splits['divs'])
    print('Done!')

    if fundamentals_writer:
        print('Writing fundamentals...')
        fundamentals_writer.write(
            _fundamentals_iter(assets_to_sids)
        )
        print('Done!')

def asset_to_sid_map(asset_finder, symbols):
    assets_to_sids = {}

    if asset_finder:
        next_free_sid = asset_finder.get_max_sid() + 1
        for symbol in symbols:
            try:
                asset = asset_finder.lookup_symbol(symbol, pd.Timestamp(date.today(), tz='UTC'))
                assets_to_sids[symbol] = int(asset)
            except (SymbolNotFound, SidsNotFound) as e:
                assets_to_sids[symbol] = next_free_sid
                next_free_sid = next_free_sid + 1

        return assets_to_sids

    for i in range(len(symbols)):
        assets_to_sids[symbols[i]] = i

    return assets_to_sids

def tiingo_metadata(tickers='ALL', asset_finder=None):
    tickers_df = pd.DataFrame(CLIENT.list_stock_tickers())

    tickers_df.to_csv('/tmp/tickers.csv')

    not_found = set()
    if tickers == 'ALL':
        tickers_df = tickers_df.loc[
            (tickers_df['exchange'].isin(['NYSE', 'NASDAQ'])) &
            (tickers_df['assetType'] == 'Stock')
        ]
    else :
        tickers_df = tickers_df.loc[tickers_df['ticker'].isin(tickers)]
        not_found = set(tickers) - set(tickers_df['ticker']) 

    tickers_df['startDate'] = pd.to_datetime(tickers_df['startDate'])
    tickers_df['endDate'] = pd.to_datetime(tickers_df['endDate'])
    tickers_df.dropna(inplace=True)

    # we currently don't support it when a symbol is held by more than
    # one security at a time. for the ones duplicated, we choose to
    # get the currently traded
    duplicates = tickers_df.loc[tickers_df.duplicated(subset=['ticker'])]
    tickers_df.drop_duplicates(subset=['ticker'], inplace=True)
    tickers_df = tickers_df[~tickers_df['ticker'].isin(duplicates['ticker'])]

    tickers_df.drop(columns=['assetType', 'priceCurrency'], inplace=True)

    to_query = duplicates['ticker'].values.tolist() + list(not_found)
    
    new_tickers = []
    with maybe_show_progress(to_query, True,
            item_show_func=lambda e: e if e is None else str(e),
            label='Retrieving metadata for duplicate tickers: ') as it:

        for ticker in it:
            try:
                new_ticker = {}
                ticker_meta = CLIENT.get_ticker_metadata(ticker)
                new_ticker['ticker'] = ticker_meta['ticker']
                new_ticker['exchange'] = ticker_meta['exchangeCode']
                new_ticker['startDate'] = pd.to_datetime(ticker_meta['startDate'])
                new_ticker['endDate'] = pd.to_datetime(ticker_meta['endDate'])
                new_tickers.append(new_ticker)
            except RestClientError:
                print(f'Warning no metadata for {ticker}, skipping...')

    tickers_df = pd.concat([tickers_df, pd.DataFrame(new_tickers)])

    tickers_df.dropna(inplace=True)
    tickers_df.reset_index(inplace=True)
    tickers_df.drop(columns=['index'], inplace=True)

    tickers_df.rename(columns={
        'ticker': 'symbol',
        'startDate': 'start_date',
        'endDate': 'end_date'
        }
        , inplace=True)

    assets_to_sids = asset_to_sid_map(asset_finder, tickers_df['symbol'].values)

    tickers_df['sid'] = [assets_to_sids[symbol] for symbol in tickers_df['symbol']]
    tickers_df.index = tickers_df['sid']
    tickers_df.drop(columns=['sid'], inplace=True)

    tickers_df['first_traded'] = tickers_df['start_date']
    tickers_df['auto_close_date'] = tickers_df['end_date'] + Timedelta(days=1)

    return tickers_df, assets_to_sids

def _pricing_iter(symbols, divs_splits, show_progress, metadata, sids_written, assets_to_sids={}):
    start_date = pd.to_datetime('2000-1-1', utc=True)
    end_date = pd.to_datetime('today', utc=True) + Timedelta(days=20)

    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')
    sessions = cal.sessions_in_range(start_date, end_date)

    with maybe_show_progress(
            symbols, show_progress,
            item_show_func=lambda e: e if e is None else str(e),
            label='Loading tiingo pricing data: ') as it:

        for symbol in it:
            sid = assets_to_sids[symbol]

            try:
                df = pd.DataFrame(CLIENT.get_ticker_price(
                    symbol,
                    fmt='json',
                    startDate=start_date,
                    frequency='daily'
                ))

                if df.empty:
                    print(f'No data for {symbol}, skpping...')
                    continue

                df.index = pd.to_datetime(df['date'], utc=True)
                df.drop(columns=[
                    'date', 'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'adjVolume'
                    ], inplace=True)
                df.rename(columns={'splitFactor': 'split', 'divCash': 'dividend'}, inplace=True)
                df = fill_daily_gaps(df)
                df = drop_extra_sessions(df)

                if 'split' in df.columns:
                    tmp = 1. / df[df['split'] != 1.0]['split']
                    split = DataFrame(data=tmp.index.tz_convert(None).tolist(),
                                      columns=['effective_date'])
                    split['ratio'] = tmp.tolist()
                    split['sid'] = sid

                    splits = divs_splits['splits']
                    index = Index(range(splits.shape[0],
                                        splits.shape[0] + split.shape[0]))
                    split.set_index(index, inplace=True)
                    divs_splits['splits'] = splits.append(split)

                if 'dividend' in df.columns:
                    # ex_date   amount  sid record_date declared_date pay_date
                    tmp = df[df['dividend'] != 0.0]['dividend']
                    div = DataFrame(data=tmp.index.tz_convert(None).tolist(), columns=['ex_date'])

                    natValue = pd.to_datetime('1800-1-1')
                    div['record_date'] = natValue
                    div['declared_date'] = natValue

                    div['pay_date'] = [sessions[sessions.get_loc(ex_date) + 10].tz_convert(None) for ex_date in div['ex_date']]

                    div['amount'] = tmp.tolist()
                    div['sid'] = sid

                    divs = divs_splits['divs']
                    ind = Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                    div.set_index(ind, inplace=True)

                    divs_splits['divs'] = divs.append(div)

            except KeyboardInterrupt:
                exit()

            except Exception as e:
                print(f'\nException for symbol {symbol}')
                print(e)

            sids_written.append(sid)
            yield sid, df

def _fundamentals_iter(assets_to_sids):
    with maybe_show_progress(assets_to_sids.items(), True,
            item_show_func=lambda e: e if e is None else str(e),
            label='Loading fundamental-data for: ') as it:

          for symbol, sid in it:
              f_daily = CLIENT.get_fundamentals_daily(symbol)
              f_statements = CLIENT.get_fundamentals_statements(symbol, asReported=True)
              
              statements_data = []
              for sheet in f_statements:
                  sheet_date = pd.to_datetime(sheet['date'], utc=True)
                  for section, data in sheet['statementData'].items():
                      statement_df = pd.DataFrame(data)
                      statement_df['date'] = sheet_date

                      # append an artifical row to be able to determine from which statement
                      # the data came from
                      statementDateField = pd.DataFrame([
                          {'dataCode':'statementDate', 'value':int(sheet_date.to_datetime64()), 'date':sheet_date}])

                      statement_df = statement_df.append(statementDateField, ignore_index=True)
                      statements_data.append(statement_df)

              if statements_data:
                  statements_data = pd.concat(statements_data)
                  statements_data.rename(columns={'dataCode': 'name'}, inplace=True)
                  statements_data.set_index(['date', 'name'], inplace=True)
                  
                  # some data is available from different sections and so duplicated
                  statements_data = statements_data.loc[~statements_data.index.duplicated()]
                  statements_data = statements_data.unstack()
                  statements_data.columns = statements_data.columns.droplevel(0)
              else:
                  statements_data = pd.DataFrame()

              if f_daily:
                  daily_data = pd.DataFrame(f_daily)
                  daily_data['date'] = pd.to_datetime(daily_data['date'])
                  daily_data.set_index('date', inplace=True)
              else: 
                  daily_data = pd.DataFrame()

              pd.concat([pd.DataFrame(), pd.DataFrame()])

              df = pd.concat([daily_data, statements_data])

              if df.empty:
                  print(f'\nNo data for {symbol}, skipping...')
                  continue

              df = df.stack().reset_index(name='value')
              df['id'] = sid
              df.rename(columns={'level_1': 'name'}, inplace=True)

              yield df


def fill_daily_gaps(df):
    """
    filling missing data. logic:
    1. get start date and end date from df. (caveat: if the missing dates are at the edges this will not work)
    2. use trading calendars to get all session dates between start and end
    3. use difference() to get only missing dates.
    4. add those dates to the original df with NaN
    5. dividends get 0 and split gets 1 (meaning no split happened)
    6. all the rest get ffill of the close value.
    7. volume get 0
    :param df:
    :return:
    """
    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')
    sessions = cal.sessions_in_range(df.index[0], df.index[-1])

    if len(df.index) == len(sessions):
        return df

    to_fill = sessions.difference(df.index)
    if len(to_fill) == 0:
        return df

    df = df.append(pd.DataFrame(index=to_fill)).sort_index()

    # forward-fill these values regularly
    df.close.fillna(method='ffill', inplace=True)
    df.dividend.fillna(0, inplace=True)
    df.split.fillna(1, inplace=True)
    df.volume.fillna(0, inplace=True)
    df.open.fillna(df.close, inplace=True)
    df.high.fillna(df.close, inplace=True)
    df.low.fillna(df.close, inplace=True)

    filled = len(to_fill)
    print(f'\nWarning! Filled {filled} empty values!')

    return df

def drop_extra_sessions(df):
    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')
    sessions = cal.sessions_in_range(df.index[0], df.index[-1])

    if len(df.index) == len(sessions):
        return df

    to_drop = df.index.difference(sessions)
    if len(to_drop) == 0:
        return df

    df.drop(to_drop, inplace=True)

    dropped = len(to_drop)
    print(f'\nWarning! Dropped {dropped} extra sessions!')

    return df

if __name__ == '__main__':
    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')

    start_date = pd.Timestamp('1999-11-1', tz='utc')
    end_date = pd.Timestamp(date.today() - timedelta(days=1), tz='utc')

    while not cal.is_session(end_date):
        end_date -= timedelta(days=1)

    print('ingesting tiingo-data from: ' + str(start_date) + ' to: ' + str(end_date))

    start_time = time.time()

    register(
        'tiingo',
        tiingo_bundle,
        calendar_name='NYSE',
        start_session=start_date,
        end_session=end_date
    )

    assets_version = ((),)[0]  # just a weird way to create an empty tuple
    bundles_module.ingest(
        "tiingo",
        os.environ,
        assets_versions=assets_version,
        show_progress=True,
    )

    print("--- %s seconds ---" % (time.time() - start_time))

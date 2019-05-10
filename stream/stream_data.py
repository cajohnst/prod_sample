# '''
# Live trading implementation of the ETN strategy

# -CJ March 2018

# '''
# import logging 
# import argparse
import datetime as dt
# from datetime import timedelta  
# import pandas as pd
# import numpy as np 
# import threading 
# from threading import Event
# import time
# from dateutil.relativedelta import relativedelta
# from tzlocal import get_localzone
# from pytz import timezone
# import sys
from threading import Event 

from src.stream.base.base import BaseStreaming
from src.stream.time.time import TimePeriodDimensionStreaming
from src.stream.rank.rank import StreamRank
from src.stream.level.level import StreamLevel
from src.stream.hist.dk.hist import DukascopyHistoricalBarsStream
from src.stream.hist.resample import ResampleHistoricalBars

def initialize_bars_updated_dict(base, vendor_cd):
    bars_updated = {}
    for i in base.timeperiod_id_to_aggregate_table_dict[vendor_cd]:
        bars_updated[i] = False
    return bars_updated 

def stream_all_dukascopy(base, top_event):
    bars_updated = initialize_bars_updated_dict(base, "DK")
    dk_hist = DukascopyHistoricalBarsStream(base, top_event, bars_updated=bars_updated)
    dk_hist.stream_dk_one_min_data_thread()
    resample = ResampleHistoricalBars(base, top_event, "DK", bars_updated=bars_updated)
    resample.stream_resample_threadpool()
    dk_rank = StreamRank(base, top_event, "DK", bars_updated=bars_updated)
    dk_rank.stream_rank_data_threadpool()
    dk_level = StreamLevel(base, top_event, "DK", bars_updated=bars_updated)
    dk_level.stream_levels_data_threadpool()

# from src.ib_trader.db_connection import DBConnection
# from src.ib_trader.ib_streaming_data import IBStreamingData
# from src.ib_trader.ib_historical_data import IBHistoricalData
# from src.configuration.email_alerts import (rank_update_exception_alert, level_update_exception_alert)
# # from src.configuration.email_alerts import send_simple_message
# # import src.configuration.email_recipients_list as email_recipients_list
# from src.configuration.db.connect_args import (read_connection_args, write_connection_args) 
# from src.configuration.db import MySQLEngine
# from src.load_dukascopy.dukascopy_reader import DukascopyReader 
    
# class SimTickData(ETNLiveStrategyDataStreaming):
#     def __init__(self, top_level_event, log_instance, sync=False, sync_inter=None):
#         super().__init__(top_level_event, log_instance)
#         self.sync = sync # sync to production db's
#         self.sync_inter = sync_inter # interval which to sync if "sync" is True 
#         self.chunksize = 10000 # num rows to write to db in a single query 
    
#     def initialize_last_update_dict(self):
#         self.last_curr_pair_update_ts = {}

#         for curr_pair_id in self.currency_pair_id_dict:
#             self.last_curr_pair_update_ts[curr_pair_id] = time.time()

#     def pull_1_min_ex_rate_aggs(self, begin_timestamp, end_timestamp, currency_pair_id):
#         query = ("SELECT "
#                     "tpd.timePeriodStartTs, "
#                     "cer.currencyPairId, "
#                     "cer.currencyPairExchangeRateInitial, "
#                     "c.currencyPipStandardPrecision "
#                  "FROM "
#                     "AltCurrencyPair1MinExchangeRateAggregate cer "
#                         "JOIN "
#                     "TimePeriodDimension tpd ON cer.timePeriodDimensionId = tpd.timePeriodDimensionId "
#                         "JOIN "
#                     "CurrencyPair cp ON cer.currencyPairId = cp.currencyPairId "
#                         "JOIN "
#                     "Currency c ON cp.quoteCurrencyId = c.currencyId "
#                  "WHERE "
#                     "cer.currencyPairExchangeRateAvailableInd = 1 "
#                         f"AND cer.currencyPairId = {currency_pair_id} "
#                         f"AND tpd.timePeriodStartTs >= '{begin_timestamp}' "
#                         f"AND tpd.timePeriodEndTs < '{end_timestamp}'; "
#         )
#         #"ORDER BY tpd.timePeriodStartTs; "
#         one_min_aggregates = self.read_query_from_db(query)

#         return one_min_aggregates

#     @staticmethod
#     def create_tick(one_min_aggregates, max_dist_from_midpoint=1.0):
#         tick_data = pd.DataFrame()

#         tick_data["currencyPairId"] = one_min_aggregates["currencyPairId"]
#         tick_data["currencyPairExchangeRate"] = one_min_aggregates["currencyPairExchangeRateInitial"]
#         tick_data["currencyPairExchangeRateTs"] = one_min_aggregates["timePeriodStartTs"].astype(str)
#         tick_data["currencyPipStandardPrecision"] = one_min_aggregates["currencyPipStandardPrecision"]
#         #simulate random spread
#         tick_data["random_spread"] = np.random.uniform(0.1, max_dist_from_midpoint, len(tick_data.index))

#         tick_data['spread_in_pips'] = tick_data['random_spread'] * (1/10**(tick_data['currencyPipStandardPrecision']))

#         tick_data['currencyPairExchangeRateBidRate'] = \
#             tick_data['currencyPairExchangeRate'] - tick_data['spread_in_pips']
        
#         tick_data['currencyPairExchangeRateAskRate'] = \
#             tick_data['currencyPairExchangeRate'] + tick_data['spread_in_pips']
        
#         tick_data.drop(columns=['random_spread', 'spread_in_pips', 'currencyPipStandardPrecision'], inplace=True)
        
#         return tick_data 

#     def simulate_tick_data(self):
#         for curr_pair_id in self.currency_pair_id_dict:
#             # last_update_ts = self.last_curr_pair_update_ts[curr_pair_id]
#             # if time.time() - last_update_ts > 60:
#             end_timestamp = self.current_est_ts
#             begin_timestamp = self.calc_begin_ts(curr_pair_id)
#             self.log.info(f"Querying DB for 1-min aggs for currency pair {curr_pair_id}")
#             one_min_aggs = self.pull_1_min_ex_rate_aggs(
#                 begin_timestamp, 
#                 end_timestamp, 
#                 curr_pair_id 
#             )
#             self.log.info(f"Completed query for 1-min aggs for currency pair {curr_pair_id}")
#             if not one_min_aggs.empty:
#                 tick_data = self.create_tick(one_min_aggs, max_dist_from_midpoint=1.0)
#                 for i in range(0, len(tick_data), self.chunksize):
#                     chunk = tick_data.iloc[i:i+self.chunksize]
#                     self.insert_table_to_db("AltCurrencyPairExchangeRate", chunk)
#                     perc_comp = round((i+self.chunksize)/len(tick_data)*100, 2)
#                     self.log.info(
#                         f"tick simulation for curr_pair_id {curr_pair_id} is {perc_comp}% uploaded."
#                     )
#             # self.last_curr_pair_update_ts[curr_pair_id] = time.time()
#         self.log.info(
#             f"Ticks inserted to db for period {begin_timestamp} -- {end_timestamp}"
#         )

#     def get_last_timestamp_in_db(self, currency_pair_id:int):
#         query = ("SELECT "
#                       "currencyPairExchangeRateTs "
#                  "FROM "
#                       "AltCurrencyPairExchangeRate "
#                  "WHERE "
#                       f"currencyPairId = {currency_pair_id} "
#                  "ORDER BY currencyPairExchangeRateTs DESC "
#                  "LIMIT 1"
#         )
#         last_ts = self.read_query_from_db(query)
#         if last_ts.empty:
#             return dt.datetime(2007, 5, 31, 23, 59)
#         else:
#             return last_ts.loc[0, "currencyPairExchangeRateTs"]

#     def calc_begin_ts(self, curr_pair_id):
#         last_db_timestamp = self.get_last_timestamp_in_db(curr_pair_id)
#         return last_db_timestamp + timedelta(minutes=1)

#     def simulate_tick_data_at_interval(self):
#         sim_tick = Event()
#         self.initialize_last_update_dict()
#         while True:
#             self.update_curr_timestamp_thread()
#             sim_tick_event_cond = not sim_tick.is_set()
#             top_level_event_cond = not self.top_level_event.is_set()

#             if not (sim_tick_event_cond & top_level_event_cond):
#                 break
#             try:
#                 self.simulate_tick_data()
#             except Exception as ex:
#                 sim_tick.set()
#             sim_tick.wait(0.1)        

#     def simulate_tick_data_thread(self):
#         t = threading.Thread(target= self.simulate_tick_data_at_interval,
#                              kwargs={})
#         t.setDaemon(True)
#         t.start()

# def start_ib_historical_data_streaming(db_export=True):
#     #begin streaming historical data for forex instruments to aggregate tables
#     ib_hist_data = IBHistoricalData(db_export=db_export)
#     ib_hist_data.start(start_time=None, 
#                        end_time=None, 
#                        req_realtime_updates=True, 
#                        instrument_list=None)

# def start_live_tick_data_streaming(db_conn, log_instance, db_export=True): 
#     #begin streaming tick data for forex instruments to CurrencyPairExchangeRate    
#     ib_tick_data = IBStreamingData(db_conn, log_instance, db_export=db_export, parent=None)
    
#     ib_tick_data.start()

# def start_dk_historical_data_streaming():
#     log_file = "logs/dk_reader.log"
#     log = setup_logger("dk_reader", log_file)
#     dk_reader = DukascopyReader(log)
#     dk_reader.main()

# def run_ticks(top_event, sim_tick_bool=False):  
#     sim_tick_log_file = "logs/tick_streaming.log"
#     tick_stream_log = setup_logger("tick_stream", sim_tick_log_file)
#     #tick data streaming
#     if sim_tick_bool:
#         sim_tick = SimTickData(top_event, tick_stream_log)
#         sim_tick.simulate_tick_data_thread()
#     else:
#         db_conn = DBConnection()
#         start_live_tick_data_streaming(db_conn, tick_stream_log, db_export=True)

# def stream_ib(top_event):
#     pass 

# def stream_dk(top_event):
#     pass

# def run_ranks(top_event, data_src):
#     rank_log_file = "logs/rank_streaming.log"
#     rank_log = setup_logger("rank_log", rank_log_file)
#     calc_ranks = CalculateRank(top_event, rank_log)
#     calc_ranks.calc_rank_data_threadpool()

# def run_levels(top_event):
#     levels_log_file = "logs/levels_streaming.log"
#     levels_log = setup_logger("levels_log", levels_log_file)
#     calc_levels = CalculateLevels(top_event, levels_log)
#     calc_levels.calc_levels_data_threadpool()

# def str2bool(val):
#     return val.lower() in ("y", "yes", "t", "true", "1")

# def main():
#     top_event = Event()
#     parser = argparse.ArgumentParser(description="Run data streaming with the following flags")
#     parser.add_argument(
#             '-st',
#             '--simTick',
#             type=str2bool,
#             nargs=1,
#             default=[False],
#             help="Set flag to derive tick data from past historical data rather than stream live."
#     )
#     parser.add_argument(
#             '-dsrc',
#             '--dataSource',
#             nargs='*',
#             default=["IB"],
#             help="Make updates using this source(s). DK, Dukascopy, IB, InteractiveBrokers are recognized sources."
#     )
#     parser.add_argument(
#             '-s',
#             '--sync',
#             type=str2bool,
#             nargs=1,
#             default=[False],
#             help='Set flag to sync to production DBs.'
#     )
#     parser.add_argument(
#             '-si',
#             '--syncInterval',
#             type = int,
#             nargs =1,
#             default =[None],
#             help ='Interval seconds between syncs of production DBs.',
#             required = ('--sync' in sys.argv) | ('-s' in sys.argv)
#             )
#     args = vars(parser.parse_args())
#     sim_tick_bool = args["simTick"][0]
#     data_src_list = args["dataSource"]
#     sync = args["sync"][0]
#     sync_int = args["syncInterval"][0]

#     if ("IB" in data_src_list) | ("InteractiveBrokers" in data_src_list):
#         # historical data streaming for IB
#         start_ib_historical_data_streaming()
#     top_event.wait(1200)
#     run_ticks(top_event, sim_tick_bool=sim_tick_bool)
#     run_ranks(top_event)
#     run_levels(top_event)
#     while True:
#         try:
#             if top_event.is_set():
#                 top_event.wait(0.1)
#             else:
#                 break
#         except KeyboardInterrupt:
#             top_event.set()

# def run_main_thread():
#     t = threading.Thread(target=main, kwargs={})
#     t.setDaemon(True)
#     t.start()

if __name__ == "__main__":
    top_event = Event()
    dta_src_list = ["DK"]
    base = BaseStreaming(top_event, dta_src_list, use_utc=False)
    stream_all_dukascopy(base, top_event)
    import pdb; pdb.set_trace()    
    ib_level = StreamLevel(base, top_event, "IB")
    ib_level.stream_levels_data_threadpool()
    ib_rank = StreamRank(base, top_event, "IB")
    ib_rank.stream_rank_data_threadpool()

    








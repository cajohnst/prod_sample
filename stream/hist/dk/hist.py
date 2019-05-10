import pandas as pd
import requests
import lzma 
import struct 
import os
import sys 
import logging
import time
import threading  
import datetime as dt 

from threading import Event
from datetime import timedelta as td  

from src.stream.utils import utils
from src.stream.hist.base import BaseHistoricalBars
from src.stream.exceptions.exceptions import GeneralHistException 

class DukascopyHistoricalBarsStream(BaseHistoricalBars):
    """
    DESCRIPTION
    -----------
    A class utilizing the base class BaseStreaming and inheriting directly from  
    the BaseHistoricalBars class for streaming aggregated currency pair exchange 
    rate data from the Dukascopy data source.

    MODIFICATIONS
    -------------
    Created : 4/29/19

    AUTHORS
    -------
    CJ
    """
    def __init__(self, BaseStreaming, top_event, use_utc=False, bars_updated=None):
        super().__init__(BaseStreaming, top_event, use_utc=use_utc)
        self.bars_updated = bars_updated
        self.initialize_dk_stream()
        
    def initialize_dk_stream(self):
        """
        DESCRIPTION
        -----------
        Initialize a log instance and get all necessary parameters pertaining 
        specifically to Dukascopy historical bars streaming. 

        MODIFICATIONS
        -------------
        Created : 4/25/19
        """
        self.establish_log_instance()
        self.struct_bi_format()
        self.format_base_url()
        self.format_base_tmp_filepath()
        self.get_agg_tbl_name()
        self.assign_exch_rate_df_col_names()
        self.vendor_id = self.get_vendor_id("DK")
        self.dk_bars_event = Event()
        self.alert_event = Event() 

    def struct_bi_format(self):
        """
        DESCRIPTION
        -----------
        Assign the binary format type for struct module.

        MODIFICATIONS
        -------------
        Created : 4/28/19
        """
        self.struct_bi_fmt = '>5L1f' # format for binary columns (5 Long(Int)s and 1 Float)

    def format_base_tmp_filepath(self):
        """
        DESCRIPTION
        -----------
        Assign the base tmp filepath to which binary files will be written.

        MODIFICATIONS
        -------------
        Created : 4/28/19
        """
        self.base_tmp_filepath = f"{os.getcwd()}/src/stream/hist/dk/tmp"

    def format_base_url(self):
        """
        DESCRIPTION
        -----------
        The base url path for Dukascopy data requests.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        """
        self.base_url = "https://datafeed.dukascopy.com/datafeed/"

    def get_agg_tbl_name(self):
        """
        DESCRIPTION
        -----------
        Get the 1-minute timeperiod type table name from the base inheritance

        MODIFICATIONS
        -------------
        Created : 4/30/19
        """
        self.agg_tbl_name = self.base.timeperiod_id_to_aggregate_table_dict["DK"][1]

    def establish_log_instance(self):
        """
        DESCRIPTION
        -----------
        Establish a logging instance for the class instance

        NOTES
        -----
        This log will be referenced for all data sources

        RETURNS
        -------
        log instance assigned to self

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/26/19
            - Adapted for streaming dukascopy historical bars class from the 
              base strategy class.
        """
        self.dk_bars_log = logging.getLogger("dk_bars")
        self.dk_bars_log.info("Dukascopy historical bars streaming logger has been established.")

    def format_url_request_date(self, req_date):
        """
        DESCRIPTION
        -----------
        Format the request date as Dukascopy interprets the date format. 
        (Leading 0's on months and days, months for some reason begin with 0
        and not 1, then format as year/month/day).

        PARAMETERS
        ----------
        req_date : dt.datetime
            A datetime or timestamp object for which to request 1 day's worth of 
            data from Dukascopy.

        RETURNS
        -------
        A string formatted date with Dukascopy date specifications.

        MODIFICATIONS
        -------------
        Created : 4/28/19
        """
        year = req_date.year
        month = "0" + str(req_date.month - 1) if req_date.month < 10 else str(req_date.month - 1)
        day = "0" + str(req_date.day) if req_date.day < 10 else str(req_date.day)
        return f"{year}/{month}/{day}"
    
    def construct_req_body(self, curr_pair_cd, req_date):
        """
        DESCRIPTION
        -----------
        Construct a request body to use to download binary files from Dukascopy

        PARAMETERS
        ----------
        curr_pair_cd : str
            A currency pair code to be used as the first portion of the url header
        req_date : dt.datetime
            A datetime or timestamp object for which to request the specific date
            file to download.
        
        RETURNS
        -------
        A formatted request body to request the download of a necessary binary file.

        MODIFICATIONS
        -------------
        Created ; 4/29/19
        """
        formatted_date = self.format_url_request_date(req_date)
        return self.base_url + f"{curr_pair_cd}/{formatted_date}/BID_candles_min_1.bi5"

    def write_resp_to_temp(self, resp, curr_pair_cd):
        """
        DESCRIPTION
        -----------
        Write the results of a exchange rate data request to a temporary file 
        location (stored as binary)

        PARAMETERS
        ----------
        curr_pair_cd : str 
            A currency pair code used to save the binary file (or overwrite the 
            existing file for the currency pair)

        RETURNS
        -------
        filename : str 
            The name of the file to which the request response was saved.

        MODIFICATIONS
        -------------
        Created ; 4/29/19
        """
        filename = f"{self.base_tmp_filepath}/{curr_pair_cd}.bi5"
        with open(filename, 'wb+') as f:
            f.write(resp.content)
            f.close()
        return filename

    def assign_exch_rate_df_col_names(self):
        """
        DESCRIPTION
        -----------
        Assign the names of columns in the order they are provided from uploading
        the Dukascopy binary file.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        self.col_names = [
                "timePeriodStartTs", 
                "currencyPairExchangeRateInitial", 
                "currencyPairExchangeRateMax", 
                "currencyPairExchangeRateMin", 
                "currencyPairExchangeRateFinal", 
                "volume"
        ]

    def bi5_to_df(self, filepath):
        """
        DESCRIPTION
        -----------
        Read and translate a binary file at the given filepath into a pandas
        dataframe instance with column names specified in `col_names`

        PARAMETERS
        ----------
        filepath : str 
            A string representing the file path of a binary type file. (.bi5)

        RETURNS
        -------
        A pandas dataframe instance translated from the contents of the binary file.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        chunk_size = struct.calcsize(self.struct_bi_fmt)
        data = []
        with lzma.open(filepath) as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    data.append(struct.unpack(self.struct_bi_fmt, chunk))
                else:
                    break
        return pd.DataFrame(data, columns=self.col_names)

    def download(self, curr_pair_id, dates_list):
        """
        DESCRIPTION
        -----------
        A wrapper function for requesting, translating, and formatting 1-minute
        interval exchange rate candle data from the Dukascopy server to a pandas
        dataframe instance.

        PARAMETERS
        ----------
        curr_pair_id : int
            An integer value representing the currency pair associated in the 
            database for which to request data from Dukascopy for
        dates_lst: list 
            A list containing all dates for which to request from Dukascopy for 
            candlestick data to aggregate into a single dataframe

        RETURNS
        -------
        An aggregated pandas dataframe containing currency pair exchange rate
        data over all requested dates and formatted to be written to the database.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        curr_pair_cd = self.base.currency_pair_id_map[curr_pair_id]
        df = pd.DataFrame()
        for date in dates_list:
            url = self.construct_req_body(curr_pair_cd, date) # construct the request body with currency pair name and date
            try:
                resp = utils.make_request(url) # make request
                resp.raise_for_status()  
            except requests.exceptions.RequestException as e:
                self.dk_bars_log.exception(e)
                break 
            fname = self.write_resp_to_temp(resp, curr_pair_cd) # write binary response to file
            tmp = self.bi5_to_df(fname) # convert binary file to dataframe
            self.adjust_df_timestamp_col(tmp, date)
            df = utils.concat_rows(df, tmp)
        utils.reset_index(df)
        return df 

    @staticmethod
    def generate_dates_list(beg_date, end_date):
        """
        DESCRIPTION
        -----------
        Generate a list of dates to request data from Dukascopy.

        PARAMETERS
        ----------
        beg_date : dt.datetime
            A datetime or timestamp object representing the beginning of a range of
            dates.
        end_date : dt.datetime
            A datetime or timestamp object reprenting the ending of a range of dates.

        RETURNS
        -------
        A list of dates iterating by 1 day from the beginning to the end date (inclusive)

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        return [beg_date + td(days=x) for x in range(0, (end_date-beg_date).days)]

    @staticmethod
    def adjust_df_timestamp_col(df, beg_date): 
        """
        DESCRIPTION
        -----------
        Convert the binary file timestamp (seconds from begin day (00:00) to a 
        readable format for MySQL 
        
        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance
        begin_date : dt.datetime
            A datetime or timestamp object representing the beginning timestamp 
            of a set of data

        NOTES
        -----
        Modifies the existing timePeriodStartTs column of df in place.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        df["timePeriodStartTs"] = [beg_date + td(seconds=s) for s in df["timePeriodStartTs"]]

    def format_exch_rate_precision(self, df, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Format exchange rate with decimal point placement (Dukascopy data provided as 
        5 or 6 digit integer)

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance
        curr_pair_id : int
            An integer value representing the associated currency pair in the database.

        NOTES
        -----
        Modifies the exchange rate columns of the dataframe in place.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        quote_curr_id = self.base.currency_pair_dict[curr_pair_id][1] # get the quote currency id of the given currency pair
        prec = self.base.currency_pip_precision_dict[quote_curr_id] # get the precision of the quote currency
        for col in df:
            if "ExchangeRate" in col:
                df[col] = df[col] * (10**((prec+1)*-1)) 

    def format(self, df, curr_pair_id):
        """
        DESCRIPTION
        -----------
        A wrapper function containing all functions for formatting the pandas
        dataframe once it is translated from the binary file so it may be MySQL
        readable.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance
        curr_pair_id : int
            An integer value representing the associated currency pair in the database.

        NOTES
        -----
        All formatting functions are applied in place.

        MODIFICATIONS
        -------------
        Created ; 4/29/19
        Modified : 4/30/19
            - Added conditional for dataframe not being empty.
        """
        if not df.empty:
            self.format_exch_rate_precision(df, curr_pair_id)
            self.format_datetime(df)
            self.add_curr_pair_id(df, curr_pair_id)
            self.add_vendor_id(df, self.vendor_id)
            self.add_available_ind(df)
            utils.drop_df_cols(df, ["volume"])

    def write_completion_to_log(self, curr_pair_id, last_update_ts, to_ts):
        """
        DESCRIPTION
        -----------
        Write a log entry for notice of completion of the update to currency pair
        exchange rate 1-minute aggregates to the current timestamp.

        PARAMETERS
        ----------
        curr_pair_id : int
            An integer value representing the currency pair id associated in the 
            database.
        last_update_ts : dt.datetime
            A datetime or timestamp instance representing the beginning of the 
            range of dates for which the update took place.
        to_ts : dt.datetime
            A datetime or timestamp instance representing the current timestamp
            and end of the range of values for which the update took place.

        MODIFICATIONS
        -------------
        Created : 5/1/19
        """
        curr_pair_cd = self.base.currency_pair_id_map[curr_pair_id]
        self.dk_bars_log.info(
                f"{curr_pair_cd} streaming in {self.agg_tbl_name} "
                f"has completed for {last_update_ts} -- {to_ts}."
        )

    def general_exception_actions(self):
        """
        DESCRIPTION
        -----------
        The following are actions taken when an exception is found in the try/except
        block in the main function.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        """
        exc_line_no = sys.exc_info()[-1].tb_lineno
        self.dk_bars_log.exception(f"An exception occurred on line {exc_line_no}.")
        if not self.alert_event.is_set():
            GeneralHistException(self.vendor_id, exc_line_no)
            self.alert_event.set()
        self.dk_bars_event.set()

    def main(self, to_ts=None):
        """
        DESCRIPTION
        -----------
        A wrapper function performing requests to the Dukascopy server, formatting
        the binary file received from Dukascopy into a pandas dataframe that is 
        writable to MySQL for 1-minute currency pair exchange rate aggregated data
        updated to the current time.

        PARAMETERS
        ----------
        to_ts : dt.datetime (defualt=None)
            A datetime or timestamp instance representing the end of the date range
            for which to update the database.
            If None, the default operation will be to update to the current timestamp.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        Modified : 4/30/19
            - Added conditionals for continuing loop in the case that df is empty 
             after calling download and if either df or tpd_df does not contain 
             the column timePeriodStartTs.
        Modified : 5/2/19
            - Some refactoring to reduce some of the bulk in the main function by 
              creating wrapper functions for exception handling and logging.
            - Added a to_ts parameters specifying the end of the date range to 
              update the database.
        """
        try:
            curr_est, curr_utc = self.base.est_ts, self.base.utc_ts # ts in both UTC and EST
            if to_ts is None:
                to_ts = self.base.get_db_ts()
            for curr_pair_id in self.base.currency_pair_dict:
                last_update_ts = self.get_last_update_ts(
                        self.agg_tbl_name, 
                        curr_pair_id
                ) # get last update ts for curr pair as in db
                last_update_utc = utils.convert_ts_to_utc(last_update_ts) # convert db ts to UTC
                dates_list = self.generate_dates_list(
                        utils.round_down_to_nearest_day(last_update_utc),
                        utils.round_down_to_nearest_day(curr_utc)
                ) # generate dates from last UTC ts to curr UTC ts
                df = self.download(
                        curr_pair_id, 
                        dates_list
                ) # download available data for all dates
                if df.empty:
                    continue 
                self.format(df, curr_pair_id) # format all data into proper MySQL format
                tpd_df = self.get_avail_timeperiod_dimensions(
                        utils.strip_tzinfo(last_update_ts),
                        utils.strip_tzinfo(utils.round_down_to_nearest_day(to_ts))
                ) # generate a dataframe of timeperiod dimensions within the range of the 
                  # last update and current timestamp
                if not (("timePeriodStartTs" in df) & ("timePeriodStartTs" in tpd_df)):
                    continue 
                df = utils.merge(tpd_df, df, on="timePeriodStartTs", how="left")
                df = self.filter_curr_pair_avail_rows(df)
                db_data_df = self.get_avail_data_in_period(
                        self.agg_tbl_name,
                        utils.strip_tzinfo(last_update_ts),
                        utils.strip_tzinfo(utils.round_down_to_nearest_day(to_ts)),
                        curr_pair_id
                )
                df = self.base.remove_duplicated_tpd_ids(db_data_df, df)
                utils.drop_df_cols(df, ["timePeriodStartTs"])
                df = utils.set_null_vals_to_none(df)
                self.base.write_df_in_chunks(
                        df, 
                        self.agg_tbl_name, 
                        chunksize=10000
                ) # write dataframe in chunks to the database.
                self.write_completion_to_log(
                        curr_pair_id,
                        last_update_ts,
                        to_ts
                )
        except Exception as ex:
            self.general_exception_actions(self.vendor_id)

    def modify_bars_updated_status(self):
        """
        DESCRIPTION
        -----------
        The bars updated boolean flag is used in tandem when streaming multiple
        classes at once.  For instance, the Dukascopy historical bars, resampling,
        level creation, and rank creation all in one.  This boolean flag tells
        the other classes to wait until one-minute bars are completed before 
        opening their threads.

        MODIFICATIONS
        -------------
        Created ; 5/2/19
        """
        if isinstance(self.bars_updated, dict):
            self.bars_updated[1] = True if self.bars_updated[1] == False else False

    def run_at_interval(self):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual
        keyboard interrupts have been triggered, else running the Dukascopy streaming
        `main` function and the subsequent wait loop function.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        Modified : 5/2/19
            - Added iter_count and modify_bars_updated_status to create a boolean
              flag which is watched by outside class threads to wait for one-minute
              bar completion before moving to resampling or level or rank calculations.
        """
        iter_count = 0
        while True:
            top_level_event_cond = not self.top_event.is_set()
            dk_event_cond = not self.dk_bars_event.is_set()
            if not (top_level_event_cond & dk_event_cond):
                self.break_explanation()
                break
            if iter_count > 0:
                self.modify_bars_updated_status() 
            self.main()
            self.modify_bars_updated_status() 
            self.wait_loop()
            iter_count += 1

    def break_explanation(self):
        """
        DESCRIPTION
        -----------
        Create a log message detailing the reasoning for a break in a Dukascopy
        streaming thread, whether it be a global keyboard interrupt (top event set)
        or an exception hit (dk bars event set).

        MODIFICATIONS
        -------------
        Created : 4/30/19
        """
        msg = "An unexplained error "
        if self.top_event.is_set():
            msg = "The top event has been flagged and "
        elif self.dk_bars_event.is_set():
            msg = "The dukascopy bars event has been flagged and "
        self.dk_bars_log.info(msg + f"has caused the thread to close.")      

    def wait_loop(self):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual 
        keyboard interrupts have been triggered, else activating a sleep period
        until triggered by the time elapsed greater than the necessary wait period.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        """
        wait_secs = self.base.calc_wait_seconds("1-day", use_utc=True, wait_seconds=300)
        self.dk_bars_log.info(f"{wait_secs} seconds until next update to {self.agg_tbl_name} table.")
        start_wait_time = time.time()
        while True:
            if self.top_event.is_set():
                break
            elif self.dk_bars_event.is_set():
                break  
            elif time.time() - start_wait_time > wait_secs:
                break 
            else:
                self.dk_bars_event.wait(0.1)

    def stream_dk_one_min_data_thread(self):
        """
        DESCRIPTION
        -----------
        Generate a thread to operate Dukascopy exchange rate data streaming at the
        necessary (1-minute) interval.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        """
        t = threading.Thread(name=f"DK_1_min_bars_thread",
                             target=self.run_at_interval, 
                             kwargs={}
        ) # open thread for each necessary level timeperiod type
        t.setDaemon(True)
        t.start()
        self.dk_bars_log.info(f"{t.name} has opened.")


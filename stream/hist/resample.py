import sys
import pandas as pd
import logging 
import time 
import threading 

from threading import Event
from datetime import timedelta as td 
from dateutil.relativedelta import relativedelta as rd 

from src.stream.utils import utils
from src.stream.hist.base import BaseHistoricalBars
from src.stream.exceptions.exceptions import GeneralHistException

class ResampleHistoricalBars(BaseHistoricalBars):
    """
    DESCRIPTION
    -----------
    A class that inherits directly from BaseHistoricalBars which serves as the 
    overall class for all resampling of historical bars to currency pair exchange
    rate aggregate tables.

    MODIFICATIONS
    -------------
    Created : 5/1/19
    """
    def __init__(self, BaseStreaming, top_event, vendor_cd, use_utc=False, bars_updated=None):
        super().__init__(BaseStreaming, top_event, use_utc=use_utc) 
        self.bars_updated = bars_updated
        self.vendor_cd = vendor_cd
        self.initialize_resample() 

    def initialize_resample(self):
        """
        DESCRIPTION
        -----------
        Initialize a log instance and get all necessary parameters pertaining 
        specifically to resampling vendor one-minute interval tables. 

        MODIFICATIONS
        -------------
        Created : 5/1/19
        """ 
        self.vendor_id = self.get_vendor_id(self.vendor_cd)
        self.establish_log_instance()
        self.res_event = Event()
        self.alert_event = Event()
        self.begin_weekday = 6

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
        if self.vendor_cd == "DK":
            name = "dk_bars"
        elif self.vendor_cd == "IB":
            name = "ib_bars"
        else:
            raise ValueError(f"Vendor Code {self.vendor_cd} is not currently supported.")
        self.res_log = logging.getLogger(name)
        self.res_log.info(f"{self.vendor_cd} historical bars log has been established for resampling.")

    def resample_at_tpt(self, df, pd_tpt_cd):
        """
        DESCRIPTION
        -----------
        Resample the one minute interval dataframe to the desired timeperiod 
        frequency.  The df must contain a timePeriodStartTs column to be converted
        to a datetime index for the new resampled frame, then reset.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance containing one-minute interval exchange
            rate aggregate data to be used to resample.
        pd_tpt_cd : str
            A string value representing the pandas recognized frequency for resampling
            which matches the desired timeperiod type in the database.

        RETURNS
        -------
        resample_df : pd.DataFrame
            A pandas dataframe instance containing resampled currency pair exchange
            rate aggregate values.

        MODIFICATIONS
        -------------
        Created : 5/2/19
        """
        utils.set_index(df, ["timePeriodStartTs"])
        resample_df = pd.DataFrame()
        resample_df["currencyPairExchangeRateInitial"] = \
            df["currencyPairExchangeRateInitial"].resample(f"{pd_tpt_cd}", label="left", closed="left").first()
        resample_df["currencyPairExchangeRateMax"] = \
            df["currencyPairExchangeRateMax"].resample(f"{pd_tpt_cd}", label="left", closed="left").max()
        resample_df["currencyPairExchangeRateMin"] = \
            df["currencyPairExchangeRateMin"].resample(f"{pd_tpt_cd}", label="left", closed="left").min()
        resample_df["currencyPairExchangeRateFinal"] = \
            df["currencyPairExchangeRateFinal"].resample(f"{pd_tpt_cd}", label="left", closed="left").last()
        utils.reset_index(resample_df, drop=False)
        return resample_df

    def format_resample(self, df, tpt_id, curr_pair_id):
        """
        DESCRIPTION
        -----------
        A wrapper function containing all functions with intent to modify the 
        resampled dataframe with added fields which serve as flags for the database.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance containing resampled currency pair exchange
            rate aggregates.
        tpt_id : int
            An integer value representing the timeperiod type id associated in the 
            database.
        curr_pair_id : int
            An integer value representing the currency pair id associated in the
            database.

        MODIFICATIONS
        -------------
        Created : 5/1/19
        """ 
        self.add_curr_pair_id(df, curr_pair_id)
        self.add_vendor_id(df, self.vendor_id)
        self.add_available_ind(df)

    def get_tbl_names(self, tpt_id):
        """
        DESCRIPTION
        -----------
        Look up and return the names of the associated database tables and code 
        types necessary for running the `resample` function.

        PARAMETERS
        ----------
        tpt_id : int
            An integer value representing the timeperiod type id associated in the 
            database.

        RETURNS
        -------
        agg_tbl_name : str 
            A string value representing the associated aggregate table for the given
            vendor and timeperiod type id.
        one_min_tbl_name : str 
            A string value representing the associated one-minute aggregate table for 
            the given vendor.
        pd_tpt_cd : str 
            A string value representing the pandas frequency code type for the 
            associated database timeperiod type id.

        MODIFICATIONS
        -------------
        Created : 5/1/19
        """
        agg_tbl_name = self.base.timeperiod_id_to_aggregate_table_dict[self.vendor_cd][tpt_id]
        one_min_tbl_name = self.base.timeperiod_id_to_aggregate_table_dict[self.vendor_cd][1]
        pd_tpt_cd = self.base.timeperiod_id_to_pd_freq_dict[tpt_id]
        return agg_tbl_name, one_min_tbl_name, pd_tpt_cd

    def write_completion_to_log(self, agg_tbl_name, curr_pair_id, last_update_ts, to_ts):
        """
        DESCRIPTION
        -----------
        Write a log entry for notice of completion of the resampling update to 
        currency pair exchange rate aggregates up to the current timestamp.

        PARAMETERS
        ----------
        agg_tbl_name : str 
            A string value representing the database table for which the resample
            update was made.
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
        self.res_log.info(
                f"{curr_pair_cd} resample in {agg_tbl_name} "
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
        self.res_log.exception(f"An exception occurred on line {exc_line_no}.")
        if not self.alert_event.is_set():
            GeneralHistException(self.vendor_id, exc_line_no)
            self.alert_event.set()
        self.res_event.set()

    def resample(self, tpt_id, to_ts=None):
        """
        DESCRIPTION
        -----------
        A wrapper function containing all of the resampling work, from gathering
        the last updated timestamp of the table/currency pair, requesting one-minute
        aggregates for the timeperiod, resampling the one-minute aggregates, joining 
        on available timeperiod dimensions for the date range, formatting the newly
        resampled aggregates, and finally writing these new aggregates to the database.

        PARAMETERS
        ----------
        to_ts : dt.datetime (defualt=None)
            A datetime or timestamp instance representing the end of the date range
            for which to update the database.
            If None, the default operation will be to update to the current timestamp.

        MODIFICATIONS
        -------------
        Created : 5/1/19
        Modified : 5/2/19
            - Some refactoring to reduce some of the bulk in the resample function by 
              creating wrapper functions for exception handling and logging.
            - Added a to_ts parameters specifying the end of the date range to 
              update the database. (Useful for the initial resample)
        """
        agg_tbl_name, one_min_tbl_name, pd_tpt_cd = self.get_tbl_names(tpt_id)
        try:
            if to_ts is None:
                to_ts = self.base.get_db_ts()
            for curr_pair_id in self.base.currency_pair_dict:
                last_update_ts = self.get_last_update_ts(
                        agg_tbl_name,
                        curr_pair_id
                ) # get last update to agg table (tz aware)
                one_min_df = self.get_avail_data_in_period(
                        one_min_tbl_name,
                        utils.strip_tzinfo(last_update_ts),
                        utils.strip_tzinfo(utils.round_down_to_nearest_day(to_ts)),
                        curr_pair_id
                ) # get available highest frequency data to be resampled at the 
                  # appropriate interval
                if one_min_df.empty:
                    return 
                tpd_df = self.get_avail_timeperiod_dimensions(
                        utils.strip_tzinfo(last_update_ts),
                        utils.strip_tzinfo(utils.round_down_to_nearest_day(to_ts)),
                        tpt_id=tpt_id
                ) # generate a dataframe of timeperiod dimensions within the 
                  # range of the last update and current timestamp
                if not (("timePeriodStartTs" in one_min_df) & ("timePeriodStartTs" in tpd_df)):
                    return  
                db_data_df = self.get_avail_data_in_period(
                        agg_tbl_name,
                        utils.strip_tzinfo(last_update_ts),
                        utils.strip_tzinfo(utils.round_down_to_nearest_day(to_ts)),
                        curr_pair_id
                ) # get current data for the given interval from the table which resamples
                  # will be written to.
                res_df = self.resample_at_tpt(one_min_df, pd_tpt_cd)
                self.format_resample(res_df, tpt_id, curr_pair_id)
                res_df = utils.merge(tpd_df, res_df, on="timePeriodStartTs", how="left")
                res_df = self.filter_curr_pair_avail_rows(res_df)
                res_df = self.base.remove_duplicated_tpd_ids(db_data_df, res_df)
                utils.drop_df_cols(res_df, ["timePeriodStartTs"])
                res_df = utils.set_null_vals_to_none(res_df)
                self.base.write_df_in_chunks(
                        res_df, 
                        agg_tbl_name, 
                        chunksize=10000
                ) # write dataframe in chunks to the database.
                self.write_completion_to_log(
                        agg_tbl_name, 
                        curr_pair_id, 
                        last_update_ts, 
                        to_ts
                )
        except Exception as ex:
            self.general_exception_actions()

    def get_next_update_time(self, ts, tpt_id):
        """
        DESCRIPTION
        -----------
        Calculate the beginning timestamp of the next timeperiod dimension based on 
        the current timestamp or timestamp provided.

        PARAMETERS
        ----------
        ts : dt.datetime, dt.timestamp
            A datetime or timestamp object representing the beginning timestamp 
            of the next timeperiod dimension from the current timestamp or timestamp
            provided.
        tpt_id : int 
            An integer representing the timeperiod type id association created in 
            the database.

        RETURNS
        -------
        A timestamp or datetime object representing the beginning timestamp of the
        next timeperiod dimension based on the timeperiod type provided.

        MODIFICATIONS
        -------------
        Created : 4/25/19
        Modified : 5/1/19
            - Adapted from the TimePeriodDimensionStreaming class.
        """
        ts = ts.replace(second=0, microsecond=0)
        if tpt_id == 1:
            next_update_ts = ts + td(minutes=1)
        elif tpt_id == 2:
            next_update_ts = ts + td(minutes=5 + 5 - ts.minute%5)
        elif tpt_id == 3:
            next_update_ts = ts + td(minutes=15 + 15 - ts.minute%15)
        elif tpt_id == 4:
            next_update_ts = ts + td(minutes=30 + 30 - ts.minute%30)
        elif tpt_id == 5:
            next_update_ts = (ts + td(hours=1)).replace(minute=0)
        elif tpt_id == 6:
            next_update_ts = (ts + td(hours=4 + 4 - ts.hour%4)).replace(minute=0)
        elif tpt_id == 7:
            next_update_ts = (ts + td(days=1)).replace(hour=0, minute=0)
        elif tpt_id == 8:
            next_update_ts = (ts + td(days=self.begin_weekday - ts.weekday())).replace(hour=0, minute=0)
        elif tpt_id == 9:
            next_update_ts = ts.replace(day=1, hour=0, minute=0) + rd(months=1)
        return next_update_ts

    def modify_bars_updated_status(self, tpt_id):
        """
        DESCRIPTION
        -----------
        The bars updated boolean flag is used in tandem when streaming multiple
        classes at once.  For instance, the resampling, level creation, and rank 
        creation all in one.  This boolean flag tells the other classes to wait 
        until the resampling of interval bars are completed before opening their 
        associated threads.

        MODIFICATIONS
        -------------
        Created : 5/2/19
        """
        if isinstance(self.bars_updated, dict):
            self.bars_updated[tpt_id] = True if self.bars_updated[tpt_id] == False else False

    def check_one_min_bar_status(self):
        """
        DESCRIPTION
        -----------
        Monitor the status of one-minute bar updates when streaming the one-minute
        Dukascopy data and simultaenously resampling.

        MODIFICATIONS
        -------------
        Created : 5/2/19
        """
        if isinstance(self.bars_updated, dict):
            return self.bars_updated[1]
        else:
            return True 

    def run_at_interval(self, tpt_id):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual
        keyboard interrupts have been triggered, else running the Dukascopy streaming
        `resample` function and the subsequent wait loop function.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        Modified : 5/1/19
            - Adapted from the Dukascopy streaming module for the generalized resampling
              class.
        Modified : 5/2/19
            - Added iter_count and modify_bars_updated_status to create a boolean
              flag which is watched by outside class threads to wait for resample
              bar completion before moving to level or rank calculations.
        """
        iter_count = 0
        while True:
            top_level_event_cond = not self.top_event.is_set()
            res_event_cond = not self.res_event.is_set()
            if not (top_level_event_cond & res_event_cond):
                self.break_explanation()
                break
            if not self.check_one_min_bar_status():
                self.res_event.wait(0.1)
                continue 
            if iter_count > 0:
                self.modify_bars_updated_status(tpt_id)
            self.resample(tpt_id)
            self.modify_bars_updated_status(tpt_id)
            self.wait_loop(tpt_id)
            iter_count += 1

    def break_explanation(self):
        """
        DESCRIPTION
        -----------
        Create a log message detailing the reasoning for a break in a resampling 
        thread, whether it be a global keyboard interrupt (top event set)
        or an exception hit (res event set).

        MODIFICATIONS
        -------------
        Created : 4/30/19
        Modified : 5/1/19
            - Adapted from Dukascopy historical bars for resampling streaming.
        """
        msg = "An unexplained error "
        if self.top_event.is_set():
            msg = "The top event has been flagged and "
        elif self.res_event.is_set():
            msg = "The resample event has been flagged and "
        self.res_log.info(msg + f"has caused the thread to close.")      

    def wait_loop(self, tpt_id):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual 
        keyboard interrupts have been triggered, else activating a sleep period
        until triggered by the time elapsed greater than the necessary wait period.

        PARAMETERS
        ----------
        tpt_id : int
            An integer value representing a timeperiod type for retrieving the 
            necessary wait time by calculating the next interval timestamp.

        MODIFICATIONS
        -------------
        Created : 4/30/19
        Modified : 5/1/19 
            - Adapted from TimePeriodDimensionStreaming for resampling streaming.
        """
        curr_ts = self.base.get_db_ts()
        next_update_ts = self.get_next_update_time(curr_ts, tpt_id)
        wait_secs = (next_update_ts - curr_ts).total_seconds()
        agg_tbl_name = self.base.timeperiod_id_to_aggregate_table_dict[self.vendor_cd][tpt_id]
        self.res_log.info(f"{wait_secs} seconds until next update to {agg_tbl_name} table.")
        start_wait_time = time.time()
        while True:
            if self.top_event.is_set():
                break
            elif self.res_event.is_set():
                break  
            elif time.time() - start_wait_time > wait_secs:
                break 
            else:
                self.res_event.wait(0.1)

    def stream_resample_threadpool(self):
        """
        DESCRIPTION
        -----------
        Create threads to operate resampling streaming calculations at each timeperiod's
        necessary interval.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        Modified : 5/1/19
            - Adapted from the level class for resampling streaming.
        """
        for tpt_id in self.base.timeperiod_id_to_aggregate_table_dict[self.vendor_cd]:
            if tpt_id == 1:
                continue
            t = threading.Thread(name=f"{tpt_id}_resample_thread",
                                 target=self.run_at_interval, 
                                 kwargs={"tpt_id": tpt_id}
            ) # open thread for each necessary resample period
            t.setDaemon(True)
            t.start()
            self.res_log.info(f"{t.name} has opened.")



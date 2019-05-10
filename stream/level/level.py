import pandas as pd
import datetime as dt
import threading 
import logging
import time 
import sys 

from threading import Event
from datetime import timedelta as td 

from src.stream.utils.utils import (strip_tzinfo, round_down_to_nearest_day)
from src.stream.exceptions.exceptions import GeneralLevelException 

class StreamLevel(object):
    """
    DESCRIPTION
    -----------
    A class inheriting from the base class for streaming level updates in near real
    time given the proper updates are made to historical (OHLC) bars.

    MODIFICATIONS
    -------------
    Created : 4/23/19

    AUTHORS
    -------
    CJ
    """
    def __init__(self, BaseStreaming, top_event, dta_src, use_utc=False, bars_updated=None):
        self.base = BaseStreaming # import the running BaseStreaming instance and assign to self
        self.top_event = top_event # assign the top event instance and assign to self
        self.dta_src = dta_src
        self.use_utc = use_utc
        self.bars_updated = bars_updated 
        self.initialize_level()

    def initialize_level(self):
        """
        DESCRIPTION
        -----------
        Initialize a log instance and get all necessary parameters pertaining 
        specifically to level streaming. 

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        self.establish_log_instance()
        self.level_event = Event()
        self.alert_event = Event()
        self.keep_cols()

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
        Modified : 4/23/19
            - Adapted for streaming level class from the base strategy class.
        """
        self.level_log = logging.getLogger("level")
        self.level_log.info("level streaming logger has been established.")

    @staticmethod 
    def shift_agg_columns(aggs_df):
        """
        DESCRIPTION
        -----------
        Create two new columns representing the shift from one and two dimensions
        past to evaluate support and resistance levels under one row index value.

        PARAMETERS
        ----------
        aggs_df : pd.DataFrame
            A pandas dataframe instance containing aggregated currency pair 
            exchange rates over a range of timeperiod dimensions.

        NOTES
        -----
        Modifies the aggs_df dataframe in place

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        for col in aggs_df:
            if "ExchangeRate" in col:
                aggs_df[f"{col}_shift_1"] = aggs_df[col].shift(periods = -1)
                aggs_df[f"{col}_shift_2"] = aggs_df[col].shift(periods = -2)   

    def calc_support(self, aggs_df):
        """
        DESCRIPTION
        -----------
        Evaluate aggs_df for rows containing conditions for which a support
        level exists.  Create flag fields for the level type id (id associated 
        with support) and the exchange rate of the level.  

        PARAMETERS
        ----------
        aggs_df : pd.DataFrame
            A pandas dataframe instance containing aggregated currency pair 
            exchange rates over a range of timeperiod dimensions.

        NOTES
        -----
        Modify aggs_df in place

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """ 
        cond1 = aggs_df["currencyPairExchangeRateMin_shift_2"] > aggs_df["currencyPairExchangeRateMax"]
        cond2 = aggs_df["currencyPairExchangeRateMax_shift_1"] > aggs_df["currencyPairExchangeRateMax"]
        cond3 = aggs_df["currencyPairExchangeRateMin_shift_1"] > aggs_df["currencyPairExchangeRateMin"]
        aggs_df.loc[(cond1 & cond2 & cond3, "exchangeRateLevelTypeId")] = self.base.level_types_dict["support"]
        aggs_df.loc[(cond1 & cond2 & cond3, "currencyPairLevelExchangeRate")] = aggs_df["currencyPairExchangeRateMax"] 

    def calc_resistance(self, aggs_df):
        """
        DESCRIPTION
        -----------
        Evaluate aggs_df for rows containing conditions for which a support
        level exists.  Create flag fields for the level type id (id associated 
        with resistance) and the exchange rate of the level.  

        PARAMETERS
        ----------
        aggs_df : pd.DataFrame
            A pandas dataframe instance containing aggregated currency pair 
            exchange rates over a range of timeperiod dimensions.

        NOTES
        -----
        Modify aggs_df in place

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        cond1 = aggs_df["currencyPairExchangeRateMax_shift_2"] < aggs_df["currencyPairExchangeRateMin"]
        cond2 = aggs_df["currencyPairExchangeRateMax_shift_1"] < aggs_df["currencyPairExchangeRateMax"]
        cond3 = aggs_df["currencyPairExchangeRateMin_shift_1"] < aggs_df["currencyPairExchangeRateMin"]
        aggs_df.loc[(cond1 & cond2 & cond3, "exchangeRateLevelTypeId")] = self.base.level_types_dict["resistance"]
        aggs_df.loc[(cond1 & cond2 & cond3, "currencyPairLevelExchangeRate")] = aggs_df["currencyPairExchangeRateMin"]

    def get_last_update_time(self, tbl_name, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Query the database for the last update time of the provided currency pair id.

        PARAMETERS
        ----------
        tbl_name : str
            the name of the table in the database which to query.
        curr_pair_id : int
            the reference id of the currency pair in the given table to query.

        NOTES
        -----
        If the table is currently empty, the begin date will be set to June 1, 2007.
        This is the first datetime where exchange rate data exists in the database.

        It is necessary to pull at least three timeperiod dimensions worth of data 
        points beyond the last dimension available since a level requires three 
        timeperiod dimensions worth of data to calculate support or resistance 
        levels.  We have decided to pull two months beyond the latest timestamp
        since this will encompass the required amount for all timeperiod dimensions
        (The monthly dimension is the largest dimension).

        RETURNS
        -------
        A datetime object representing the most recent timestamp for the given 
        currency pair in the database table.

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        query = ("SELECT "
                    "tpd.timePeriodStartTs "
                 "FROM "
                f"{tbl_name} lvl "
                 "JOIN "
                    "TimePeriodDimension tpd "
                 "ON lvl.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "WHERE "
                    f"lvl.currencyPairId = {curr_pair_id} "
                 "ORDER BY tpd.timePeriodStartTs DESC LIMIT 1; "
        )
        last_update_time_df = self.base.select(query)
        min_datetime = dt.datetime(2007, 6, 1, 0, 0)
        if last_update_time_df.empty:
            return min_datetime
        else:
            # require at least 2 previous data points to create a level beyond the start ts
            db_last_update_time = last_update_time_df.loc[0, "timePeriodStartTs"] - td(days=62)
            return max(db_last_update_time, min_datetime)

    def get_existing_levels_in_time_period(self, tbl_name, begin_ts, end_ts, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Create and execute an SQL query and translate this into a pandas dataframe
        containing the existing levels data in a given table with a given currency 
        pair id over a specified date range.

        PARAMETERS
        ----------
        tbl_name : str
            The name of the table in the database for which to query.
        begin_ts : dt.datetime, timestamp
            The timestamp marking the beginning of the specified date range in 
            the SQL query.
        end_ts : dt.datetime, timestamp
            The timestamp marking the end of the specified date range in the SQL
            query.
        curr_pair_id : int
            An integer value representing the associated id with the currency
            pair in the SQL query.

        RETURNS
        -------
        A pandas dataframe instance translated from the results of the SQL query.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        query = ("SELECT "
                    "tpd.timePeriodDimensionId, "
                    "lt.currencyPairId, "
                    "lt.currencyPairLevelAvailableInd, "
                    "lt.exchangeRateLevelTypeId, "
                    "lt.currencyPairLevelExchangeRate "
                f"FROM {tbl_name} lt "
                 "JOIN TimePeriodDimension tpd "
                    "ON lt.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "WHERE "
                    f"tpd.timePeriodStartTs >= '{begin_ts}' "
                    f"AND tpd.timePeriodEndTs < '{end_ts}' "
                    f"AND lt.currencyPairId = {curr_pair_id} "
                 "ORDER BY tpd.timePeriodStartTs; "
        )
        return self.base.select(query)

    def get_hist_agg_data(self, tbl_name, begin_ts, end_ts, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Create and execute an SQL query and translate the results into a pandas
        dataframe containing historical bars from a given table matching a given 
        currency pair id over a specified date range.

        PARAMETERS
        ----------
        tbl_name : str
            The name of the table in the database for which to query.
        begin_ts : dt.datetime, timestamp
            The timestamp marking the beginning of the specified date range in 
            the SQL query.
        end_ts : dt.datetime, timestamp
            The timestamp marking the end of the specified date range in the SQL
            query.
        curr_pair_id : int
            An integer value representing the associated id with the currency pair
            in the SQL query.

        RETURNS
        -------
        A pandas dataframe instance translated from the results of the SQL query.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        query = ("SELECT "
                    "agg.currencyPairExchangeRateMax, "
                    "agg.currencyPairExchangeRateMin, "
                    "agg.currencyPairId, "
                    "tpd.timePeriodDimensionId "
                f"FROM {tbl_name} agg "
                 "JOIN TimePeriodDimension tpd "
                    "ON agg.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "WHERE "
                    f"tpd.timePeriodStartTs >= '{begin_ts}' "
                    f"AND tpd.timePeriodEndTs < '{end_ts}' "
                     "AND tpd.timePeriodHasOperationalMarketInd = 1 "
                    f"AND agg.currencyPairId = {curr_pair_id} "
                 "ORDER BY tpd.timePeriodStartTs; "
        )
        return self.base.select(query)

    @staticmethod
    def filter_null_levels(aggs_df):
        """
        DESCRIPTION
        -----------
        Return a pandas dataframe copy containing rows in which the "Level Exchange Rate"
        field is not null.

        PARAMETERS
        ----------
        aggs_df : pd.DataFrame
            A pandas dataframe containing aggregated currency pair exchange rate
            data over a range of timeperiod dimensions.

        RETURNS
        -------
        A pandas dataframe copy containing rows in which the "Level Exchange Rate"
        field is not null.

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        return aggs_df.loc[aggs_df.currencyPairLevelExchangeRate.notnull()]

    @staticmethod
    def add_level_avail_ind(lvls_df):
        """
        DESCRIPTION
        -----------
        Create a currencyPairLevelAvailableInd field in the lvls_df dataframe.
        Assign the value 1 to all rows.

        PARAMETERS
        ----------
        lvls_df : pd.DataFrame
            A new pandas dataframe instance containing values from aggs_df filtered
            for null values and without columns which will not be written to the
            database.

        NOTES
        -----
        Modifies lvls_df in place

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        lvls_df["currencyPairLevelAvailableInd"] = 1

    def keep_cols(self):
        """
        DESCRIPTION
        -----------
        Create a list of columns to be used in filtering dataframe columns down 
        to those necessary for writing to the database.

        NOTES
        -----
        Assigns keep_cols to self

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        self.keep_cols = ["timePeriodDimensionId", 
                          "currencyPairId", 
                          "exchangeRateLevelTypeId", 
                          "currencyPairLevelExchangeRate",
                          "currencyPairLevelAvailableInd"]

    def filter_keep_cols(self, lvls_df):
        """
        DESCRIPTION
        -----------
        Return a pandas dataframe copy containing only the list of desired fields
        found in self.keep_cols

        SEE ALSO
        --------
        keep_cols

        PARAMETERS
        ----------
        lvls_df : pd.DataFrame
            A pandas dataframe instance containing newly calculated support and 
            resistance levels.

        RETURNS
        -------
        A dataframe copy containing only the desired columns found in keep_cols

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        return lvls_df[self.keep_cols]

    def filter_levels(self, aggs_df):
        """
        DESCRIPTION
        -----------
        A wrapper function for filtering null values from the exchange rate dataframe
        and eliminating columns which will not be written to the database.

        PARAMETERS
        ----------
        aggs_df : pd.DataFrame
            A pandas dataframe instance containing aggregated currency pair exchange
            rate data over a range of timeperiod dimensions

        RETURNS
        -------
        lvls_df : pd.DataFrame
            A new pandas dataframe instance containing values from aggs_df filtered
            for null values and without columns which will not be written to the
            database.

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        self.add_level_avail_ind(aggs_df) # add level available ind field to aggs_df
        lvls_df = self.filter_null_levels(aggs_df) # filter null exchange rate level values
        lvls_df = self.filter_keep_cols(lvls_df) # filter columns to those written to database
        return lvls_df

    def calc_levels(self, tpt_id):
        """
        DESCRIPTION
        -----------
        A wrapper function which encompasses the process of currency pair level 
        formation; from querying the necessary data from the database, performing 
        level calculations on the data, comparing against existing data in the 
        database to ensure no duplicate entries are written, and finally writing 
        new level data to the database.

        PARAMETERS
        ----------
        tpt_id : int 
            An integer value representing the timeperiod type associated with level
            and aggregate tables in the database to perform and write new level
            calculations to.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        lvl_tbl_name = self.base.timeperiod_id_to_levels_table_dict[self.dta_src][tpt_id]
        agg_tbl_name = self.base.timeperiod_id_to_aggregate_table_dict[self.dta_src][tpt_id]
        try:
            for curr_pair_id in self.base.currency_pair_dict:
                begin_ts = self.get_last_update_time(lvl_tbl_name, curr_pair_id)
                if self.use_utc == True:
                    end_ts = strip_tzinfo(
                            round_down_to_nearest_day(self.base.utc_ts)
                    )
                elif self.use_utc == False:
                    end_ts = strip_tzinfo(
                            round_down_to_nearest_day(self.base.est_ts)
                    )
                ex_levels = self.get_existing_levels_in_time_period(
                    lvl_tbl_name,
                    begin_ts, 
                    end_ts,
                    curr_pair_id
                )
                ex_aggs = self.get_hist_agg_data(
                        agg_tbl_name,
                        begin_ts,
                        end_ts,
                        curr_pair_id
                    )
                if not ex_aggs.empty:
                    self.shift_agg_columns(ex_aggs)
                    self.calc_support(ex_aggs)
                    self.calc_resistance(ex_aggs)
                    lvls_df = self.filter_levels(ex_aggs)
                    rem_dups = self.base.remove_duplicated_tpd_ids(ex_levels, lvls_df)
                    if not rem_dups.empty:
                        self.base.insert(lvl_tbl_name, rem_dups)
                curr_pair_cd = self.base.currency_pair_id_map[curr_pair_id]
                self.level_log.info(f"{lvl_tbl_name} levels updated for {curr_pair_cd} from {begin_ts} -- {end_ts}.")
        except Exception as ex:
            exc_line_no = sys.exc_info()[-1].tb_lineno
            self.level_log.exception(f"An exception occurred on line {exc_line_no}.")
            if not self.alert_event.is_set():
                GeneralLevelException(exc_line_no)
                self.alert_event.set()
            self.level_event.set()

    def check_interval_bar_status(self, tpt_id):
        """
        DESCRIPTION
        -----------
        Monitor the status of specified interval bar updates when streaming 
        levels and resampling data simultaneously.
        
        PARAMETERS
        ----------
        tpt_id : int
            An interger value representing the timeperiod type id association 
            created in the database.

        MODIFICATIONS
        -------------
        Created : 5/2/19
        """
        if isinstance(self.bars_updated, dict):
            return self.bars_updated[tpt_id]
        else:
            return True 

    def calc_levels_at_interval(self, tpt_id):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual
        keyboard interrupts have been triggered, else running the level calculation
        function and the subsequent wait loop function.

        PARAMETERS
        ----------
        tpt_id : int 
            An integer representing the timeperiod type id association created in 
            the database.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        Modified : 5/2/19
            - Added check_interval_bar_status for monitoring the status of the
              necessary resampled 
        """
        tpt_cd = self.base.timeperiod_types_dict[tpt_id]
        while True:
            top_level_event_cond = not self.top_event.is_set()
            calc_levels_event_cond = not self.level_event.is_set()
            if not (top_level_event_cond & calc_levels_event_cond):
                self.break_explanation(tpt_cd)
                break
            if not self.check_interval_bar_status(tpt_id):
                self.level_event.wait(0.1)
                continue 
            self.calc_levels(tpt_id)
            self.wait_loop(tpt_cd)

    def break_explanation(self, tpt_cd):
        """
        DESCRIPTION
        -----------
        Create a log message detailing the reasoning for a break in a level 
        calculation thread, whether it be a global keyboard interrupt (top event set)
        or an exception hit (level event set) and specifying the thread which has been 
        broken.

        PARAMETERS
        ----------
        tpt_cd : str
            A string representing a timeperiod type in a readable code format for 
            specifying the thread which has closed in the level log.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        msg = "An unexplained error "
        if self.top_event.is_set():
            msg = "The top event has been flagged and "
        elif self.level_event.is_set():
            msg = "The level event has been flagged and "
        self.level_log.info(msg + f"has caused the {tpt_cd} level thread to close.")      

    def wait_loop(self, tpt_cd):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual 
        keyboard interrupts have been triggered, else activating a sleep period
        until triggered by the time elapsed greater than the necessary wait period.

        PARAMETERS
        ----------
        tpt_cd : str
            A string representing a timeperiod type in a readable code format for 
            retrieving the necessary wait time by calculating the next interval 
            timestamp.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        wait_secs = self.base.calc_wait_seconds(tpt_cd)
        self.level_log.info(f"{wait_secs} seconds until next update to {tpt_cd} level table.")
        start_wait_time = time.time()
        while True:
            if self.top_event.is_set():
                break
            elif self.level_event.is_set():
                break  
            elif time.time() - start_wait_time > wait_secs:
                break 
            else:
                self.level_event.wait(0.1)

    def stream_levels_data_threadpool(self):
        """
        DESCRIPTION
        -----------
        Create threads to operate level data streaming calculations at each timeperiod's
        necessary interval.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        for tpt_id in self.base.timeperiod_id_to_levels_table_dict[self.dta_src]:
            t = threading.Thread(name=f"{self.dta_src}_{tpt_id}_thread",
                                 target=self.calc_levels_at_interval, 
                                 kwargs={"tpt_id":tpt_id}
            ) # open thread for each necessary level timeperiod type
            t.setDaemon(True)
            t.start()
            self.level_log.info(f"{t.name} has opened.")

if __name__ == "__main__":
    top_event = Event()
    level = StreamLevel()
    level.stream_levels_data_threadpool()

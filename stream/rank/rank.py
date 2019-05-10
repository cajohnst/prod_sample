import pandas as pd
import datetime as dt
import threading 
import logging
import time
import sys  

from threading import Event 

from src.stream.utils.utils import (strip_tzinfo, round_down_to_nearest_day, concat_rows)
from src.stream.exceptions.exceptions import GeneralRankException 


class StreamRank(object):
    """
    DESCRIPTION
    -----------
    A class inheriting from the base class for streaming rank updates in near real
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
        self.initialize_rank()

    def initialize_rank(self):
        """
        DESCRIPTION
        -----------
        Initialize a log instance and get all necessary parameters pertaining 
        specifically to rank streaming. 

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        self.establish_log_instance()
        self.rank_event = Event()
        self.alert_event = Event()
        self.method_id = self.get_method_id()

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
        Modified : 4/22/19
            - Adapted for streaming rank class from the base strategy class.
        """
        self.rank_log = logging.getLogger("rank")
        self.rank_log.info("rank streaming logger has been established.")
    

    def get_method_id(self, methodCd="ETN_RL"):
        """
        DESCRIPTION
        -----------
        Get the associated method id from the database given the method code.

        PARAMETERS
        ----------
        methodCd : str (default = "ETN_RL")
            A string representing the method code for the given rank strategy.
            The default (and currently only) strategy is "ETN_RL"

        RETURNS
        -------
        methodId : int
            An integer value representing the associated id with the provided
            methodCd in the database.

        MODIFIED
        --------
        Created : 4/23/19
        """
        query = f"SELECT methodId FROM Method WHERE methodCd = '{methodCd}';"
        method_df = self.base.select(query)
        return method_df.loc[0, "methodId"]

    def get_last_update_time(self, tbl_name, curr_id):
        """
        DESCRIPTION
        -----------
        Query the database for the last update time of the provided currency id.

        PARAMETERS
        ----------
        tbl_name : str
            the name of the table in the database which to query.
        curr_id : int
            the reference id of the currency in the given table to query.

        NOTES
        -----
        If the table is currently empty, the begin date will be set to June 1, 2007.
        This is the first datetime where exchange rate data exists in the database.

        RETURNS
        -------
        A datetime object representing the most recent timestamp for the given currency 
        in the database table.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        query = ("SELECT "
                    "tpd.timePeriodStartTs "
                 "FROM "
                f"{tbl_name} rt "
                 "JOIN "
                    "TimePeriodDimension tpd "
                 "ON rt.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "WHERE "
                    f"rt.currencyId = {curr_id} "
                 "ORDER BY tpd.timePeriodStartTs DESC LIMIT 1; "
        )
        last_update_ts_df = self.base.select(query)
        if last_update_ts_df.empty:
            return dt.datetime(2007, 6, 1, 0, 0)
        else:
            return last_update_ts_df.loc[0, "timePeriodStartTs"]

    def get_existing_rankings_in_time_period(self, tbl_name, begin_ts, end_ts, curr_id):
        """
        DESCRIPTION
        -----------
        Create and execute an SQL query and translate this into a pandas dataframe
        containing the existing rank data in a given table with a given currency 
        id over a specified date range.

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
        curr_id : int
            An integer value representing the associated id with the currency
            in the SQL query.

        RETURNS
        -------
        A pandas dataframe instance translated from the results of the SQL query.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        Modified : 4/24/19 
            - changed end_ts condition to less than timePeriodEndTs over less than
              equal to timePeriodStartTs
        """
        query = ("SELECT "
                    "tpd.timePeriodDimensionId, "
                    "rt.currencyRankingOrder, "
                    "rt.methodId, "
                    "rt.currencyId, "
                    "rt.currencyRankingAvailableInd "
                f"FROM {tbl_name} rt "
                 "JOIN TimePeriodDimension tpd "
                    "ON rt.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "WHERE "
                    f"tpd.timePeriodStartTs >= '{begin_ts}' "
                    f"AND tpd.timePeriodEndTs < '{end_ts}' "
                    f"AND rt.currencyId = {curr_id} "
                 "ORDER BY tpd.timePeriodStartTs;"
        )
        return self.base.select(query)

    def get_hist_agg_data(self, tbl_name, begin_ts, end_ts, curr_id):
        """
        DESCRIPTION
        -----------
        Create and execute an SQL query and translate the results into a pandas
        dataframe containing historical bars from a given table matching a given 
        currency id over a specified date range.

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
        curr_id : int
            An integer value representing the associated id with the currency
            in the SQL query.

        RETURNS
        -------
        A pandas dataframe instance translated from the results of the SQL query.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        Modified : 4/24/19 
            - changed end_ts condition to less than timePeriodEndTs over less than
              equal to timePeriodStartTs
        """
        query = ("SELECT "
                    "tpd.timePeriodDimensionId, "
                    "tpd.timePeriodStartTs, "
                    "cp.baseCurrencyId, "
                    "agg.currencyPairExchangeRateInitial, "
                    "agg.currencyPairExchangeRateFinal "
                 f"FROM {tbl_name} agg "
                  "JOIN TimePeriodDimension tpd "
                    "ON agg.timePeriodDimensionId = tpd.timePeriodDimensionId "
                  "JOIN CurrencyPair cp " 
                    "ON agg.currencyPairId = cp.currencyPairId "
                  "WHERE "
                    f"tpd.timePeriodStartTs >= '{begin_ts}' "
                    f"AND tpd.timePeriodEndTs < '{end_ts}' "
                    f"AND tpd.timePeriodHasOperationalMarketInd = 1 "
                    f"AND (cp.baseCurrencyId = {curr_id} || cp.quoteCurrencyId = {curr_id}) "
                  "ORDER BY tpd.timePeriodStartTs; "
        )
        return self.base.select(query)

    @staticmethod 
    def get_last_hist_agg_data_timeperiod(agg_data):
        """
        DESCRIPTION
        -----------
        Retrieve the last aggregated currency pair exchange rate timeperiod 
        currently written in the database.

        PARAMETERS
        ----------
        agg_data : pd.DataFrame
            A pandas dataframe containing aggregated currency pair exchange rate data.

        RETURNS
        -------
        A pandas timestamp object representing the last timeperiod available in 
        the database currently.

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        return agg_data.timePeriodStartTs.iloc[-1]

    def get_dimension_after_last_current_dimension(self, last_tp, tpt_id):
        """
        DESCRIPTION
        -----------
        Get the next available timeperiod dimension after the dimension of the 
        most recently available aggregated currency pair exchange rate data.

        PARAMETERS
        ----------
        last_tp : pd.Timestmap
            A pandas timestamp object representing the most recent timestamp
            where aggregated currency pair exchange rate data is available
        tpt_id : int
            An integer representing the timeperiod type id association created in 
            the database.

        RETURNS
        -------
        A pandas dataframe instance translated from the results of the SQL query.
        
        MODIFICATIONS
        -------------
        Created : 4/24/19
        """ 
        query = ("SELECT "
                 "timePeriodDimensionId "
                 "FROM "
                 "TimePeriodDimension "
                 "WHERE "
                 f"timePeriodStartTs > '{last_tp}' "
                 f"AND timePeriodTypeId = {tpt_id} "
                 "AND timePeriodHasOperationalMarketInd = 1 "
                 "ORDER BY timePeriodStartTs "
                 "LIMIT 1;"
        )
        return self.base.select(query)

    def get_current_timestamp(self):
        """
        DESCRIPTION
        -----------
        Get the current timestamp rounded down to the nearest day stripped of all 
        timezone info.  Can be calculated in UTC or EST timezones, but is stipped
        down to a naive timestamp.

        RETURNS
        -------
        curr_ts : dt.datetime
            A datetime object representing the current timestamp rounded down to 
            the nearest day stripped of all timezone info.

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        if self.use_utc == True:
            curr_ts = strip_tzinfo(
                    round_down_to_nearest_day(self.base.utc_ts)
            )
        elif self.use_utc == False:
            curr_ts = strip_tzinfo(
                    round_down_to_nearest_day(self.base.est_ts)
            )
        return curr_ts

    def calc_interval_flows(self, curr_id, df):
        """
        DESCRIPTION
        -----------
        Evaulate the flow of currency pair exchange rates from the beginning to 
        end of a period in a pandas dataframe.  Create a binary field stating
        whether the base currency has appreciated or depreciated against the 
        quote currency which will be used in a group summation to "rank" currencies.
        Include an additional field stating whether the currency pair exchange rate
        value is available for the given timeperiod dimension or is null.  Make these
        modifications to the given dataframe in place.

        PARAMETERS
        ----------
        curr_id : int
            An integer value representing the associated id with the currency given
            in the database.
        df : pd.DataFrame
            A pandas dataframe instance containing currency pair exchange rate values
            associated with a given timeperiod dimension

        NOTES
        -----
        df is modified in place.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """ 
        pos_flow_cond = df["currencyPairExchangeRateFinal"] > df["currencyPairExchangeRateInitial"]
        neg_flow_cond = df["currencyPairExchangeRateFinal"] <= df["currencyPairExchangeRateInitial"]
        neutral_flow_cond = df["currencyPairExchangeRateFinal"] == df["currencyPairExchangeRateInitial"]
        base_curr_cond = df["baseCurrencyId"] == curr_id
        not_base_curr_cond = df["baseCurrencyId"] != curr_id
        miss_flow_dta_cond = df["currencyPairExchangeRateInitial"].isnull()
        no_miss_flow_dta_cond = df["currencyPairExchangeRateInitial"].notnull()
        curr_strong_cond = (
                no_miss_flow_dta_cond & \
                ((base_curr_cond & pos_flow_cond) | (not_base_curr_cond & neg_flow_cond))
        )
        curr_weak_cond = (
                (base_curr_cond & (neg_flow_cond | neutral_flow_cond)) | \
                (not_base_curr_cond & (pos_flow_cond | neutral_flow_cond)) | \
                (miss_flow_dta_cond)
        ) # currency is considered "weak" if value for currency pair is null.
        df.loc[(curr_strong_cond,"currencyRankingOrder")] = 1 # assign strong value to curr_id
        df.loc[(curr_weak_cond, "currencyRankingOrder")] = 0 # assign weak value to curr_id
        df.loc[(miss_flow_dta_cond, "currencyRankingAvailableInd")] = 1 # assign int 1 if row data is available
        df.loc[(no_miss_flow_dta_cond, "currencyRankingAvailableInd")] = 0 # assign int 0 if row data is null 

    @staticmethod
    def group_and_rank(df):
        """
        DESCRIPTION
        -----------
        Create a pandas dataframe instance by grouping from an inherited dataframe
        by time period dimension id and summing these groupings, a sum of 0 being 
        the weakest and a sum of 6 being the strongest (all cross pair data points 
        being available).

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe containing the flow directional binary values for 
            strong/weak currency in a currency pair.  

        RETURNS
        -------
        group_df : pd.DataFrame
            A pandas dataframe containing the summation of flow directional binary 
            values ranking the currency on a scale of 0-6 (0 being weakest, 6 being
            strongest) across each unique timeperiod dimension id.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        group_df = pd.DataFrame()
        group_cols = ["timePeriodDimensionId", "currencyRankingOrder", "currencyRankingAvailableInd"]
        group_df = df[group_cols].groupby(["timePeriodDimensionId"]).sum()
        group_df.reset_index(inplace=True)
        return group_df

    @staticmethod
    def shift_rank(df):
        """
        DESCRIPTION
        -----------
        Shift the currencyRankingOrder column values of a pandas dataframe by
        an offset of one row.

        NOTES
        -----
        The purpose of the shift is to match rankings of the previous month to the
        current traded month.  It is a performance optimization for sql queries with 
        joins involving levels traded and ranks for the given timeperiod.

        The pandas dataframe is modified in place.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        df["timePeriodDimensionId"] = df["timePeriodDimensionId"].shift(-1)
        df.drop(df.index[-1:], inplace=True)

    def add_curr_and_method_id(self, df, curr_id):
        """
        DESCRIPTION
        -----------
        Add currencyId and methodId fields to a pandas dataframe.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe containing the rank values
        curr_id : int 
            An integer value representing the currency id to add as a field to 
            the dataframe instance.

        NOTES
        -----
        Modifies the dataframe in place.

        MODIFICATIONS
        -------------
        Created ; 4/23/19
        """
        df["currencyId"] = curr_id 
        df["methodId"] = self.method_id 

    def calc_rank(self, tpt_id):
        """
        DESCRIPTION
        -----------
        A wrapper function which encompasses the process of ranking currencies;
        from querying the necessary data from the database, performing ranking
        calculations on the data, comparing against existing data in the database
        to ensure no duplicate entries are written, and finally writing new ranking
        data to the database.

        PARAMETERS
        ----------
        tpt_id : int 
            An integer value representing the timeperiod type associated with rank
            and aggregate tables in the database to perform and write new ranking
            calculations to.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        rnk_tbl_name = self.base.timeperiod_id_to_ranking_table_dict[self.dta_src][tpt_id] 
        agg_tbl_name = self.base.timeperiod_id_to_aggregate_table_dict[self.dta_src][tpt_id]       
        try:
            for curr_id in self.base.currency_id_dict.values():
                begin_ts = self.get_last_update_time(rnk_tbl_name, curr_id)
                end_ts = self.get_current_timestamp()
                ex_ranks = self.get_existing_rankings_in_time_period(
                    rnk_tbl_name,
                    begin_ts,
                    end_ts,
                    curr_id
                )
                ex_aggs = self.get_hist_agg_data(
                    agg_tbl_name,
                    begin_ts,
                    end_ts,
                    curr_id
                )
                if not ex_aggs.empty:
                    self.calc_interval_flows(curr_id, ex_aggs)
                    rank_df = self.group_and_rank(ex_aggs)
                    curr_tpd = self.get_last_hist_agg_data_timeperiod(ex_aggs)
                    next_tpd = self.get_dimension_after_last_current_dimension(curr_tpd, tpt_id)
                    rank_df = concat_rows(rank_df, next_tpd)
                    self.shift_rank(rank_df)
                    self.add_curr_and_method_id(rank_df, curr_id)
                    rem_dups = self.base.remove_duplicated_tpd_ids(ex_ranks, rank_df)
                    if not rem_dups.empty:
                        self.base.insert(rnk_tbl_name, rem_dups)
                curr_cd = self.base.currency_cd_dict[curr_id]
                self.rank_log.info(f"{rnk_tbl_name} rankings updated for {curr_cd} from {begin_ts} -- {end_ts}.")
        except Exception as ex:
            exc_line_no = sys.exc_info()[-1].tb_lineno
            self.rank_log.exception(f"An exception occurred on line {exc_line_no}.")
            if not self.alert_event.is_set():
                GeneralRankException(exc_line_no)
                self.alert_event.set()
            self.rank_event.set()

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

    def calc_rank_data_at_interval(self, tpt_id):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual
        keyboard interrupts have been triggered, else running the rank calculation
        function and the subsequent wait loop function.

        PARAMETERS
        ----------
        tpt_id : int 
            An integer representing the timeperiod type id association created in 
            the database.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        tpt_cd = self.base.timeperiod_types_dict[tpt_id]
        while True:
            top_level_event_cond = not self.top_event.is_set()
            calc_rank_event_cond = not self.rank_event.is_set()
            if not (top_level_event_cond & calc_rank_event_cond):
                self.break_explanation(tpt_cd)
                break
            if not self.check_interval_bar_status(tpt_id):
                self.rank_event.wait(0.1)
                continue
            self.calc_rank(tpt_id)                
            self.wait_loop(tpt_cd)
            
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
        self.rank_log.info(f"{wait_secs} seconds until next update to {tpt_cd} ranking table.")
        start_wait_time = time.time()
        while True:
            if self.top_event.is_set():
                break
            elif self.rank_event.is_set():
                break  
            elif time.time() - start_wait_time > wait_secs:
                break 
            else:
                self.rank_event.wait(0.1)

    def break_explanation(self, tpt_cd):
        """
        DESCRIPTION
        -----------
        Create a log message detailing the reasoning for a break in a rank 
        calculation thread, whether it be a global keyboard interrupt (top event set)
        or an exception hit (rank event set) and specifying the thread which has been 
        broken.

        PARAMETERS
        ----------
        tpt_cd : str
            A string representing a timeperiod type in a readable code format for 
            specifying the thread which has closed in the rank log.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        msg = "An unexplained error "
        if self.top_event.is_set():
            msg = "The top event has been flagged and "
        elif self.rank_event.is_set():
            msg = "The rank event has been flagged and "
        self.rank_log.info(msg + f"has caused the {tpt_cd} rank thread to close.")

    def stream_rank_data_threadpool(self):
        """
        DESCRIPTION
        -----------
        Create threads to operate rank data streaming calculations at each timeperiod's
        necessary interval.

        MODIFICATIONS
        -------------
        Created : 4/23/19
        """
        for tpt_id in self.base.timeperiod_id_to_ranking_table_dict[self.dta_src]:
            t = threading.Thread(name=f"{self.dta_src}_{tpt_id}_thread",
                                 target=self.calc_rank_data_at_interval, 
                                 kwargs={"tpt_id":tpt_id}
            ) # open thread for each necessary rank timeperiod type
            t.setDaemon(True)
            t.start()
            self.rank_log.info(f"{t.name} has opened.")

if __name__ == "__main__":
    top_event = Event()
    base = BaseStreaming()
    rank = StreamRank(base, top_event, "IB")
    rank.stream_rank_data_threadpool()

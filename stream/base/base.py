import logging
import time
import os
import pytz
import threading
import datetime as dt
import pandas as pd    

from threading import Event
from dateutil.relativedelta import relativedelta as rd 
from datetime import timedelta as td 

from src.configuration.db.connect_args import (read_connection_args, write_connection_args) 
from src.configuration.db import MySQLEngine
from src.stream.utils.utils import reverse_key_value, set_index  
 
class BaseStreaming(object):
    """
    DESCRIPTION
    -----------
    A base level inheritance class for level streaming, rank streaming, historical
    bars streaming from all sources, tick data simulation and streaming, and 
    database syncing.

    Modifications
    -------------
    Created 4/18/19

    AUTHORS
    -------
    CJ
    """
    def __init__(self, top_event, dta_src_lst, use_utc=False):
        self.top_event = top_event
        self.dta_src_lst = dta_src_lst
        self.use_utc = use_utc 
        self.initialize_base()

    def initialize_base(self):
        """
        DESCRIPTION
        -----------
        Initialize a log instance, db connections, then gather necessary references tables, 
        and other necessary parameters for streaming data 

        Modifications
        -------------
        Created : 4/22/19
        """
        self.establish_log_instance()

        start_ts = time.time()
        self.base_log.info("initializing db engines, strategy parameters, and reference dicts... ")

        self.initialize_engines()
        self.map_timeperiod_tables()
        self.get_reference_dicts()
        self.establish_tz_info()
        self.update_curr_timestamp_thread()

        end_ts = time.time()
        process_secs = round(end_ts - start_ts, 3)
        self.base_log.info(f"strategy initialized in {process_secs} seconds.")

    def establish_tz_info(self):
        """
        DESCRIPTION
        -----------
        Establish timezone info for UTC and EST timezones

        RETURNS
        -------
        Assigns these timezones to self

        MODIFICATIONS
        -------------
        Created : 4/22/19
        """
        self.utc = pytz.timezone("UTC")
        self.est = pytz.timezone("US/Eastern")

    def get_current_ts(self):
        """
        DESCRIPTION
        -----------
        Get the current timestamp with EST and UTC timezone info

        RETURNS
        -------
        Returns the current timestamp first with the EST timezone,
        then the UTC timezone

        MODIFICATIONS
        -------------
        Created : 4/22/19
        """
        return dt.datetime.now(self.est), dt.datetime.now(self.utc)

    def get_db_ts(self):
        """
        DESCRIPTION
        -----------
        Return the current utc timestamp if the database is specified to use utc else
        use EST (which is the default timezone for the database at this time).

        RETURNS
        -------
        A timestamp or datetime object representing the current timestamp including
        timzone information as the database is configured to utilize.

        MODIFICATIONS
        -------------
        Created : 5/1/19
        """
        return self.utc_ts if self.use_utc == True else self.est_ts # ts as would be in database

    def establish_log_instance(self):
        """
        DESCRIPTION
        -----------
        Establish a logging instance for the class

        NOTES
        -----
        This log will be references in all sub-classes

        RETURNS
        -------
        log instance assigned to self

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for streaming base class from base strategy class.
        """
        self.base_log = logging.getLogger("stream")
        self.base_log.info("base streaming logger has been established.")

    def initialize_engines(self):
        """
        DESCRIPTION
        -----------
        Initialize read and write engines connecting to the appropriate DB.

        NOTES
        -----
        Separate read and write engines are necessary in the case where
        an optimization is made to write exclusively to one DB and read from another.
        In this scenario it is required that data being written is not essential 
        to subsequent reads

        MODIFICATIONS
        -------------
        Created 1/29/19
        Modified : 4/18/19
            - Adapted for streaming base class from base strategy class.
        """
        self.read_engine = MySQLEngine(**read_connection_args)
        self.write_engine = MySQLEngine(**write_connection_args)

    def insert(self, 
                table_name, # str, 
                table_df # pd.DataFrame()
               ):
        """
        DESCRIPTION
        -----------
        Insert pandas dataframe object into DB

        PARAMETERS
        ----------
        table_name : str
            The table name given in the db as the destination for inserted data
        table_df : pd.DataFrame object
            The dataframe from which to convert to SQL

        RETURNS
        -------
        first_insert_id : int
            The first primary key id value inserted into the DB, used for marking
            appropriate id's back into the dataframe object

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modifiied : 4/18/19
            - Adapted for base streaming class from base strategy class.

        """
        with self.write_engine.connection(commit=False) as con: 
            first_insert_id = con.insert(table_name, table_df)
        return first_insert_id

    def select(self, 
                query # str
              ):
        """
        DESCRIPTION
        -----------
        Return a pandas dataframe object using a select type query on the DB

        PARAMETERS
        ----------
        query : str
            A query string to pass to the DB to return selected rows

        RETURNS
        -------
        df : pandas.DataFrame object
            Input the returned sql data into a pandas dataframe object

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modifiied : 4/18/19
            - Adapted for base streaming class from base strategy class.

        """
        with self.read_engine.connection(commit=False) as con:
            df = con.select(query)
        return df 

    def update(self, 
                query # str
              ):
        """
        DESCRIPTION
        -----------
        Given a query string, update existing rows in the DB according to the query 

        PARAMETERS
        ----------
        query : str
            Formatted SQL update query as a string object

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modifiied : 4/18/19
            - Adapted for base streaming class from base strategy class. 

        """
        with self.write_engine.connection(commit=False) as con:
            con.update(query)

    def map_timeperiod_tables(self):
        """
        DESCRIPTION
        -----------
        Create a relation between a timeperiod type id and the associated
        aggregate, level, and rank table with respect to the data source.

        RETURNS 
        -------
        Assign relational tables to self

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for multiple data sources from base strategy class.
        Modified : 4/22/19
            - Deleted rank_to_agg_tables_dict and levels_to_agg_tables_dict because
              of redundancy.
        """
        self.timeperiod_id_to_aggregate_table_dict = {
            "IB": {
                    1:"CurrencyPair1MinExchangeRateAggregate", 
                    2:"CurrencyPair5MinExchangeRateAggregate", 
                    3:"CurrencyPair15MinExchangeRateAggregate", 
                    4:"CurrencyPair30MinExchangeRateAggregate", 
                    5:"CurrencyPair1HourExchangeRateAggregate", 
                    6:"CurrencyPair4HourExchangeRateAggregate",
                    7:"CurrencyPair1DayExchangeRateAggregate", 
                    8:"CurrencyPair1WeekExchangeRateAggregate", 
                    9:"CurrencyPair1MonthExchangeRateAggregate"
            },
            "DK": {
                    1:"AltCurrencyPair1MinExchangeRateAggregate",
                    2:"AltCurrencyPair5MinExchangeRateAggregate",
                    3:"AltCurrencyPair15MinExchangeRateAggregate",
                    4:"AltCurrencyPair30MinExchangeRateAggregate", 
                    5:"AltCurrencyPair1HourExchangeRateAggregate", 
                    6:"AltCurrencyPair4HourExchangeRateAggregate",
                    7:"AltCurrencyPair1DayExchangeRateAggregate", 
                    8:"AltCurrencyPair1WeekExchangeRateAggregate", 
                    9:"AltCurrencyPair1MonthExchangeRateAggregate"
            },
            "COMP": {
                    1:"CompCurrencyPair1MinExchangeRateAggregate", 
                    2:"CompCurrencyPair5MinExchangeRateAggregate", 
                    3:"CompCurrencyPair15MinExchangeRateAggregate", 
                    4:"CompCurrencyPair30MinExchangeRateAggregate", 
                    5:"CompCurrencyPair1HourExchangeRateAggregate", 
                    6:"CompCurrencyPair4HourExchangeRateAggregate",
                    7:"CompCurrencyPair1DayExchangeRateAggregate", 
                    8:"CompCurrencyPair1WeekExchangeRateAggregate", 
                    9:"CompCurrencyPair1MonthExchangeRateAggregate"   
            }
        }
        self.timeperiod_id_to_levels_table_dict = {
            "IB": {
                    7:"CurrencyPairDailyExchangeRateLevel",
                    8:"CurrencyPairWeeklyExchangeRateLevel",
                    9:"CurrencyPairMonthlyExchangeRateLevel"
            },
            "DK": {
                    7:"AltCurrencyPairDailyExchangeRateLevel",
                    8:"AltCurrencyPairWeeklyExchangeRateLevel",
                    9:"AltCurrencyPairMonthlyExchangeRateLevel"   
            },
            "COMP": {
                    7:"CompCurrencyPairDailyExchangeRateLevel",
                    8:"CompCurrencyPairWeeklyExchangeRateLevel",
                    9:"CompCurrencyPairMonthlyExchangeRateLevel"
            }
        }
        self.timeperiod_id_to_ranking_table_dict = {
            "IB": {
                    7:"CurrencyDailyRanking",
                    8:"CurrencyWeeklyRanking",
                    9:"CurrencyMonthlyRanking"
            },
            "DK": {
                    7:"AltCurrencyDailyRanking",
                    8:"AltCurrencyWeeklyRanking",
                    9:"AltCurrencyMonthlyRanking"

            },
            "COMP": {
                    7:"CompCurrencyDailyRanking",
                    8:"CompCurrencyWeeklyRanking",
                    9:"CompCurrencyMonthlyRanking"
            }
        }

        self.timeperiod_id_to_pd_freq_dict = {
                1: '1T', 
                2: '5T', 
                3: '15T', 
                4: '30T', 
                5: 'H', 
                6: '4H', 
                7: 'D', 
                8: 'W', 
                9: 'MS'
        }
        
    def get_reference_dicts(self):
        """
        DESCRIPTION
        -----------
        A functional decomposition to get all of the necessary associations
        and mappings of relevant reference data from the DB

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for base stream class from strategy class. 
        """
        self.get_currency_id_dict()
        self.get_currency_precision_dict()
        self.get_currency_pair_id_dict()
        self.get_currency_pair_id_map()
        self.get_level_types_dict()
        self.get_timeperiod_types_dict()
        self.get_vendor_dict()
        self.reverse_currency_id_dict()

    def reverse_currency_id_dict(self):
        """
        DESCRIPTION
        -----------
        Create a reverse lookup dict of the currency id dict

        MODIFICATIONS
        -------------
        Created : 4/24/19
        """
        self.currency_cd_dict = reverse_key_value(self.currency_id_dict)
    
    def get_currency_id_dict(self, 
                table_name="Currency" # str
                             ):
        """
        DESCRIPTION
        -----------
        Create, format and execute an SQL statement to select 
        Currency reference data, and insert this into a reference
        dictionary currency_id_dict assigned to self
        
        PARAMETERS
        ----------
        table_name : str (default= "Currency")
            the name of the table located in the DB containing Currency reference
            data. 

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for base streaming class from strategy class.
        """
        query = f"SELECT * FROM {table_name}"
        currency_df = self.select(query)

        self.currency_id_dict = dict(
            zip(
                currency_df['currencyCd'], 
                currency_df['currencyId']
            )
        ) 

    def get_currency_pair_id_dict(self, 
                table_name="CurrencyPair" # str 
                                  ):
        """ 
        DESCRIPTION
        -----------
        Create, format, and execute an SQL statement to select and format this 
        information into a reference dictionary with currency pair ids as 
        keys and a tuple of their base and quote currencies as the values.
        
        PARAMETERS
        ----------
        table_name : str (default= "CurrencyPair")
            the name of the table located in the DB containing CurrencyPair reference
            data.

        RETURNS
        -------
        Assigns the reference dictionary currency_pair_id_dict to self

        NOTES
        -----
        This function differs from get_currency_pair_id_map in that it associates
        a currency pair id to its base and quote currencies, where as the map 
        associates the currency pair code "name" with its id in the DB.

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for base streaming class from base strategy class. 
        """
        query = f"SELECT * FROM {table_name}"
        currency_pair_df = self.select(query)
        self.currency_pair_dict = dict(
            zip(
                currency_pair_df['currencyPairId'],
                list(
                    zip(currency_pair_df['baseCurrencyId'], 
                        currency_pair_df['quoteCurrencyId']
                        )
                )
            )
        )

    def get_currency_pair_id_map(self):
        """
        DESCRIPTION
        -----------
        Create an association dictionary with currency pair codes as keys and the 
        associated currency pair id as the values.

        NOTES
        -----
        This function differs from get_currency_pair_id_dict in that it utilizes currencyCd
        rather than currencyId

        EXAMPLE
        -------
        {"AUDCAD": 1}

        RETURNS
        -------
        Assigns the reference dictionary currency_pair_id_map to self

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for base stream class from base strategy class.
        """
        query = ("SELECT "
                    "base_c.currencyCd as baseName, "
                    "quote_c.currencyCd as quoteName, "
                    "cp.currencyPairId "
                 "FROM CurrencyPair cp "
                 "JOIN Currency base_c "
                    "ON cp.baseCurrencyId = base_c.currencyId "
                 "JOIN Currency quote_c "
                    "ON cp.quoteCurrencyId = quote_c.currencyId;"
        )
        currency_pair_id_df = self.select(query)
        self.currency_pair_id_map = dict(
            zip(
                currency_pair_id_df["currencyPairId"], 
                currency_pair_id_df["baseName"]+currency_pair_id_df["quoteName"]
                )
        )

    def get_currency_precision_dict(self, 
                table_name="Currency" # str 
                                    ):
        """
        DESCRIPTION
        -----------
        Create, format, and execute an SQL statement to select 
        the currency precision reference data and format this 
        into a reference dictionary currency_pip_precision_dict 
        assigned to self

        PARAMETERS
        ----------
        table_name : str (default= "Currency")
            the name of the table located in the DB containing Currency reference
            data.

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for base stream class from base strategy class.
        """
        query = f"SELECT * FROM {table_name};"
        currency_df = self.select(query)
        self.currency_pip_precision_dict = dict(
            zip(
                currency_df["currencyId"],
                currency_df["currencyPipStandardPrecision"]
                )
        )

    def get_timeperiod_types_dict(self,
                table_name="TimePeriodType" # str
                                  ):
        """
        DESCRIPTION 
        -----------
        Create, format, and execute an SQL query to select the timeperiod type ids
        and their associated codes in a reference dictionary

        PARAMETERS
        ----------
        table_name : str
            the name of the table in the DB containing time period type information

        RETURNS
        -------
        Assigns the reference dictionary timeperiod_types_dict to self

        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for streaming base class from base strategy class.
        """
        query = f"SELECT * FROM {table_name}"
        timeperiod_types_df = self.select(query)
        self.timeperiod_types_dict = dict(
            zip(
                timeperiod_types_df["timePeriodTypeId"],
                timeperiod_types_df["timePeriodTypeCd"]
            )
        )

    def get_level_types_dict(self, 
                tbl_name="ExchangeRateLevelType" # str
                            ):
        """
        DESCRIPTION
        -----------
        Create, format, and execute an SQL query to select the level type codes
        and their associated level type ids in a reference dictionary

        PARAMETERS
        ----------
        table_name : str
            the name of the table in the DB containing level types information

        RETURNS
        -------
        Assigns the reference dictionary level_types_dict to self
        
        MODIFICATIONS
        -------------
        Created : 1/29/19
        Modified : 4/18/19
            - Adapted for streaming base class from base strategy class.
        """
        query = f"SELECT * FROM {tbl_name}"
        level_types_df = self.select(query)
        self.level_types_dict = dict(
            zip(
                level_types_df["exchangeRateLevelTypeCd"].str.lower(), 
                level_types_df["exchangeRateLevelTypeId"]
            )
        )

    def get_vendor_dict(self, tbl_name="Vendor"):
        """
        DESCRIPTION
        -----------
        Create, format, and execute an SQL query to select the vendor type codes
        and their associated vnedor ids in a reference dictionary.

        PARAMETERS
        ----------
        tbl_name : str (default = Vendor)
            A string value representing the name of the database table containing
            vendor reference data

        RETURNS
        -------
        Assigns the reference dictionary vendor_dict to self.

        MODIFICATIONS
        -------------
        Created : 4/26/19
        """
        query = "SELECT * FROM Vendor"
        df = self.select(query)
        self.vendor_dict = dict(
                            zip(df["vendorCd"],
                                df["vendorId"]
                            )
        )

    def update_current_timestamp(self):
        """
        DESCRIPTION
        -----------
        A loop utilized to call the get_current_ts func to update the current 
        timestamp at a frequent (1/10 sec) interval.

        NOTES
        -----
        This loop is designed to break when the top level event (primarily a 
        keyboard interrupt but possibly a system error) is flagged.

        MODIFICATIONS
        -------------
        Created : 4/22/19
        """
        while True:
            if not self.top_event.is_set():
                self.est_ts, self.utc_ts = self.get_current_ts() 
            else:
                self.base_log.info("The top level event has been flagged as 'set' in Update Current Ts Thread.")
                self.base_log.info("Update Current Ts Thread has closed.") 
                return 
            time.sleep(0.1)

    def update_curr_timestamp_thread(self):
        """
        DESCRIPTION
        -----------
        Calls the function update_current_timestamp and runs this loop in a thread
        so that other necessary streaming work may be completed simultaneously.

        NOTES
        -----
        The thread is run as a daemon thread and should complete when the external
        top level event is flagged.

        MODIFICATIONS
        -------------
        Created : 4/22/19
        """
        t = threading.Thread(target=self.update_current_timestamp,
                             kwargs={})
        t.setDaemon(True)
        t.start()

    def calc_wait_seconds(self, time_period_type_cd, use_utc=False, wait_seconds=0):
        """
        DESCRIPTION
        -----------
        Calculate the amount of time in seconds to the next interval given a
        time period frequency and the current timestamp.

        PARAMETERS
        ----------
        time_period_type_cd : str
            A string value representing the relative frequency of the interval
            used ("1-day", "1-week", "1-month")   
        wait_seconds : int (default=0)
            An optional amount of additional seconds to add to the timestamp at
            which the next interval takes place. 

        RETURNS
        -------
        A float value representing the number of seconds necessary to wait or sleep
        until the next interval timestamp is reached.

        MODIFICATIONS
        -------------
        Created : 4/22/19
        Modifed : 4/22/19
            - Incorporated option for use of UTC timestamp
        Modified : 4/30/19
            - Changed function name to calc_wait_seconds from get_next_interval_time
        """
        if use_utc == False:
            curr_ts = self.est_ts 
            begin_day = self.est_ts.replace(hour=0, minute=0, second=0, microsecond=0)
        elif use_utc == True:
            curr_ts = self.utc_ts 
            begin_day = self.utc_ts.replace(hour=0, minute=0, second=0, microsecond=0)

        if time_period_type_cd == "1-day":
            if (begin_day.weekday() <= 3) | (begin_day.weekday() == 6): # if current weekday is Sunday-Thurs
                next_interval = begin_day + td(days=1)
            else: # if current weekday is Friday-Saturday
                next_interval = begin_day.replace(hour=17 if use_utc == False else 22) + td(days=6-begin_day.weekday())
        elif time_period_type_cd == "1-week":            
            next_interval = begin_day.replace(hour=17 if use_utc == False else 22) + td(days=6-begin_day.weekday())                
        elif time_period_type_cd == "1-month":
            next_interval = begin_day.replace(day=1) + rd(months=1) 
            if next_interval.weekday() == 5: # if next interval lands on a Saturday, replace with market open Sunday
                next_interval = next_interval.replace(hour=17 if use_utc == False else 22) + td(days=1)
                print(next_interval)  
        return (next_interval - curr_ts).total_seconds() + wait_seconds
    
    @staticmethod
    def remove_duplicated_tpd_ids(curr_table, new_table):
        """
        DESCRIPTION
        -----------
        Remove duplicate time period dimension id rows from two pandas dataframes.
        The methodology is to eliminate all results which are in the new table which 
        also appear in the curr table.

        PARAMETERS
        ----------
        curr_table : pd.DataFrame
            A pandas dataframe containing the current contents of a database table
            for a specified range of datetime values.
        new_table : pd.DataFrame
            A pandas dataframe containing rows over a specified range of datetime 
            values.  (Presumed new or updated values)

        RETURNS
        -------
        rem_dups : pd.DataFrame
            A pandas dataframe containing all of the values from the new table which
            do not share a corresponding timeperiod dimension id value with a row
            previously written to the database table.

        MODIFICATIONS
        -------------
        Created : 4/22/19
        Modified : 4/24/19
            - Changed method from concatenating to removing using dimensions not in 
              curr_table series timePeriodDimensionId
        """
        rem_dups = new_table.loc[~new_table.timePeriodDimensionId.isin(curr_table.timePeriodDimensionId)] # eliminate all results in the new_table which are shared in the curr_table to avoid writing duplicates to the database
        rem_dups.reset_index(inplace=True, drop=True) # reset the index of the new dataframe with duplicates removed
        return rem_dups

    def write_df_in_chunks(self, df, tbl_name, chunksize=10000):
        """
        DESCRIPTION
        -----------
        Intended for use with large dataframes, write to database in chunks of a
        specified row length (chunksize).

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe to write to the database.
        tbl_name : str
            A string representing the name of the table for which to write in 
            the database.
        chunksize : int (default = 10000)
            An integer value representing the number of rows to write in one 
            insert query

        MODIFICATIONS
        -------------
        Created : 4/25/19
        """
        for i in range(0, len(df), chunksize):
            tmp = df.iloc[i:i+chunksize]
            self.insert(tbl_name, tmp)

if __name__ == "__main__":
    top_event = Event()
    data_src = "DK"
    base = BaseStreaming(top_event, data_src)

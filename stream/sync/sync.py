import logging
import os
import pandas as pd
import threading
import time
import sys     

from threading import Event 

from src.configuration.db import MySQLEngine
from src.stream.utils import utils 
from src.stream.exceptions.exceptions import GeneralSyncException

class ProdDBArgs:
    """
    DESCRIPTION
    -----------
    A class used to define static information for the set of databases including
    ip addresses and remote login arguments. 

    MODIFICATIONS
    -------------
    Created : 5/6/19
    """
    ips = {
        "LOCAL": {
            "internal":"127.0.0.1",
            "public":"127.0.0.1",
            "remote_port":3306
        },
        "DEV_1": {
            "internal":"10.142.0.2",
            "public":"35.196.223.86",
            "remote_port":3307
        },
        "DEV_2": {
            "internal":"10.142.0.3", 
            "public":"35.227.28.143",
            "remote_port":3308

        },
        "DEV_3": {
            "internal":"10.142.0.5",
            "public":"35.196.108.89",
            "remote_port":3309
        },
        "DEV_4": {
            "internal":"10.142.0.6",
            "public":"35.237.115.12",
            "remote_port":3310
        },
        "LIVE_1": {
            "internal":"10.142.0.4",
            "public":"35.231.64.137",
            "remote_port":3311
        }
    }
    args = {
            "user": "remote",
            "password": "remote",
            "database": "forex_etnrl",
            "auth_plugin":"mysql_native_password",
            "use_unicode": True,
            "autocommit": True,
            "reconnect_retry_count":3,
            "reconnect_retry_period":0.1,
            "pool_size":5,
            "pool_name":"mypool"
    } 
    # "host": "127.0.0.1",
    # "port": 3306,

class DB(object):
    """
    DESCRIPTION
    -----------
    A class containing functions to connect, query, and update MySQL database
    instances.

    PARAMETERS
    ----------
    serv_name : str 
        A string value representing the name of the server the database exists on.
    sync_tbls_lst : list
        A list containing the names of the tables which will be evaluated and synced.
    log_instance : logging.getLogger
        A logging instance used to log errors, exceptions, and other information
        pertaining to the database instance.
    master_db : bool (default=False)
        A boolean flag representing whether the DB instance is the master database.
    use_internal : bool (default=False)
        A boolean flag representing whether to use internal IP addresses (in case
        where all servers can be accessed within the same network)
    remote_sync : bool (default=False)
        A boolean flag representing whether the database is being accessed via 
        SSH tunnel from a remote location.
    update_method : str (default="PK")
        A string value representing the method for which to track current and last
        saved locations for database tables.  Currently supported options include 
        PK (primary key) and TS (timestamp).
    overwrite_slave : bool (default=False)
        A boolean flag representing whether to overwrite any modifications made to 
        a slave database from the last save point by syncing from the master.

    NOTES
    -----
    With the given parameter value `overwrite_slave` as True, the class 
    will prepare the database table by deleting any modifications made since the 
    previous sync was made, barring that the database is the master db.

    MODIFICATIONS
    -------------
    Created : 5/7/19
    """
    def __init__(self, serv_name, sync_tbls_lst, log_instance, master_db=False, 
                 use_internal=False, remote_sync=False, update_method="PK",
                 overwrite_slave=False):
        self.serv_name = serv_name 
        self.sync_tbls_lst = sync_tbls_lst
        self.sync_log = log_instance 
        self.master_db = master_db 
        self.remote_sync = remote_sync 
        self.update_method = update_method
        self.overwrite_slave = overwrite_slave 
        if use_internal:
            self.db_ip_add = ProdDBArgs.ips[serv_name]["internal"]
        else:
            self.db_ip_add = ProdDBArgs.ips[serv_name]["public"]
        self.initialize_db_instance()

    def initialize_db_instance(self):
        """
        DESCRIPTION
        -----------
        A wrapper function for initializing the DB instance.  Verify the table
        sync list, create the engine instance, obtain the last saved update from 
        the previous sync, obtain the current update points for all tables, 
        determine if the table has been modified, make appropriate additions or 
        deletions to tables to prepare for syncing from master to slave.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        self.create_default_sync_table_list()
        self.verify_sync_tbls_lst()
        self.create_engine()
        self.get_comp_field_name()
    
    def update_db_table_positions(self):
        self.get_last_saved_update_df()
        self.create_current_update_df()
        self.are_tbls_modified = self.eval_if_tbls_have_been_modified()
        self.initial_sync_actions()

    def create_engine(self):
        """
        DESCRIPTION
        -----------
        Create an engine instance; a pool of connections to the database described
        by the provided parameters and default arguments provided in the ProdDBArgs
        class instance.

        NOTES
        -----
        Assigns the engine as an attribute to the class.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        args = ProdDBArgs.args
        if self.remote_sync is True:
            args["host"] = "127.0.0.1"
            args["port"] = ProdDBArgs.ips[self.serv_name]["remote_port"] 
        else:
            args["port"] = 3306
            if self.master_db is True:
                args["host"] = "127.0.0.1"
            else:
                args["host"] = self.db_ip_add             
        self.engine = MySQLEngine(**args)

    def insert(self, tbl_name, tbl_df):
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
        Modified : 4/18/19
            - Adapted for base streaming class from base strategy class.
        """
        with self.engine.connection(commit=False) as con: 
            first_insert_id = con.insert(tbl_name, tbl_df)
        return first_insert_id

    def select(self, query):
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
        with self.engine.connection(commit=False) as con:
            df = con.select(query)
        return df 

    def update(self, query):
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
        with self.engine.connection(commit=False) as con:
            con.update(query)

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
        Modified : 5/7/19
            - Modified from the base streaming class for syncing databases with
              intention to transfer large amounts of data or entire tables if 
              necessary.
        """
        for i in range(0, len(df), chunksize):
            tmp = df.iloc[i:i+chunksize]
            self.insert(tbl_name, tmp)

    def create_default_sync_table_list(self):
        """
        DESCRIPTION
        -----------
        Assign default table names and their respective primary key fields in a 
        reference dictionary to be accessed and cross-checked when table sync
        requests are made.

        NOTES
        -----
        Assigns the default sync tbls dict to the class.

        MODIFICATIONS
        -------------
        Created ; 5/6/19
        """
        self.default_sync_tbls_dict = {
            "CurrencyPair1MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId", 
            "CurrencyPair5MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair15MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair30MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair1HourExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair4HourExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair1DayExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair1WeekExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyPair1MonthExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair1MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId", 
            "AltCurrencyPair5MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair15MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair30MinExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair1HourExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair4HourExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair1DayExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair1WeekExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "AltCurrencyPair1MonthExchangeRateAggregate": "currencyPairTimePeriodExchangeRateId",
            "CurrencyDailyRanking": "currencyRankingId",
            "CurrencyWeeklyRanking": "currencyRankingId",
            "CurrencyMonthlyRanking": "currencyRankingId",
            "AltCurrencyDailyRanking": "currencyRankingId",
            "AltCurrencyWeeklyRanking": "currencyRankingId",
            "AltCurrencyMonthlyRanking": "currencyRankingId",
            "CurrencyPairDailyExchangeRateLevel": "levelId",
            "CurrencyPairWeeklyExchangeRateLevel": "levelId",
            "CurrencyPairMonthlyExchangeRateLevel": "levelId",
            "AltCurrencyPairDailyExchangeRateLevel": "levelId",
            "AltCurrencyPairWeeklyExchangeRateLevel": "levelId",
            "AltCurrencyPairMonthlyExchangeRateLevel": "levelId",
            "CurrencyPairExchangeRate": "currencyPairExchangeRateId",
            "AltCurrencyPairExchangeRate": "currencyPairExchangeRateId",
            "TimePeriodDimension": "timePeriodDimensionId"
        }

    def verify_sync_tbls_lst(self):
        """
        DESCRIPTION
        -----------
        Compare the user input tables list to the default server tables list to 
        verify that all user entries are valid database table names.

        RAISES
        ------
        NameError : 
            In the case that the server name provided does not match any of the 
            default table names, raise this error to ensure the user input is 
            corrected.
        TypeError : 
            In the case that neither a list or the default None argument is passed
            by the user, raise this error to inform that the input type provided
            is not valid.

        MODIFICATIONS
        -------------
        Created : 5/6/19
        """
        if self.sync_tbls_lst is None:
            self.sync_tbls_dict = self.default_sync_tbls_dict
        elif isinstance(self.sync_tbls_lst, list):
            for tbl_name in self.sync_tbls_lst:
                if tbl_name not in self.default_sync_tbls_dict:
                    raise NameError(f"{tbl_name} is not a supported table name.")
                else:
                    self.sync_tbls_dict[tbl_name] = self.default_sync_tbls_dict[tbl_name]
        else:
            raise TypeError(f"sync tables list must be Nonetype or of type `list`.")

    def get_last_saved_update_df(self):
        """
        DESCRIPTION
        -----------
        A wrapper function intened to obtain the last saved updates for each table 
        according to the provided update method.

        NOTES
        -----
        Assigns the last update dataframe as an attribute to the class.

        RAISES
        ------
        ValueError : 
            Raise a value error if the update method selected is neither of the 
            supported methods (Primary Key, Last updated timestamp)

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        if self.update_method == "PK":
            self.create_pk_csv_locations()
            self.last_update_df = self.read_last_saved_csv(
                    self.pk_csv_fp, 
                    self.pk_csv_backup_fp
            )
        elif self.update_method == "TS":
            self.create_ts_csv_locations()
            self.last_update_df = self.read_last_saved_csv(
                self.ts_csv_fp, 
                self.ts_csv_backup_fp
            )
        else:
            raise ValueError(f"{self.update_method} is not a supported update method.")

    def get_last_update_ts(self, tbl_name):
        """
        DESCRIPTION
        -----------
        Obtain the greatest timestamp available for data in a database table.
        This is completed by joining the table with the TimePeriodDimension table
        in all except the CurrencyPairExchangeRate and AltCurrencyPairExchangeRate
        tables, which have their own independent timestamp fields.

        PARAMETERS
        ----------
        tbl_name : str
            A string value representing the name of the table in the database.

        RETURNS
        -------
        A pandas dataframe instance containing the resulting maximum timestamp
        value available in the database table.

        MODIFICATIONS
        -------------
        Created : 5/7/19 
        """
        if tbl_name not in ("CurrencyPairExchangeRate", "AltCurrencyPairExchangeRate"):
            query = ("SELECT "
                    "tpd.timePeriodStartTs "
                 "FROM "
                    f"{tbl_name} tbl "
                 "JOIN TimePeriodDimension tpd "
                    "ON tbl.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "ORDER BY tpd.timePeriodStartTs DESC "
                 "LIMIT 1;"
            )
        else:
            query = ("SELECT "
                        "currencyPairExchangeRateTs "
                     "FROM "
                        f"{tbl_name} "
                     "ORDER BY currencyPairExchangeRateTs DESC "
                     "LIMIT 1; "
            )
        return self.select(query)

    def get_last_pk(self, tbl_name, prim_key):
        """
        DESCRIPTION
        -----------
        Obtain the greatest primary key value available for a database table.

        PARAMETERS
        ----------
        tbl_name : str  
            A string value representing the name of the table in the database.
        prim_key : str 
            A string value representing the field name of the primary key in the table.

        RETURNS
        -------
        A pandas dataframe instance containing the value of the greatest primary 
        key value available in a database table.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        query = ("SELECT "
                    f"{prim_key} "
                 "FROM "
                    f"{tbl_name} "
                 f"ORDER BY {prim_key} DESC "
                 "LIMIT 1;"
        )
        return self.select(query)

    def create_current_update_df(self):
        """
        DESCRIPTION
        -----------
        A wrapper function intended to obtain the current primary key value or 
        last timestamp available for all tables in the list of tables to sync.

        NOTES
        -----
        Assigns the current update dataframe instance as an attribute of the class.

        RAISES
        ------
        ValueError : 
            Raise a value error if the update method selected is neither of the 
            supported methods (Primary Key, Last Updated Timestamp)

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        if self.update_method == "PK":
            self.create_current_pk_df()
        elif self.update_method == "TS":
            self.create_current_ts_df()
        else:
            raise ValueError(f"{self.update_method} is not a supported update method.")

    def get_comp_field_name(self):
        """
        DESCRIPTION
        -----------
        Return the value of the column name in the pandas dataframe based on the 
        update method parameter.

        MODIFICATIONS
        -------------
        Created : 5/8/19
        """
        if self.update_method == "PK":
            self.comp_field_name = "prim_key"
        elif self.update_method == "TS":
            self.comp_field_name = "last_ts"
        else:
            raise ValueError(f"{self.update_method} is not a supported update method.")

    def get_db_field_name(self, tbl_name):
        """
        DESCRIPTION
        -----------
        Return the value of the field name representing the primary key or timestamp
        column used to measure updates

        PARAMETERS
        ----------
        tbl_name : str
            A string value representing the name of the table in the database.

        NOTES
        -----
        Assigns `db_field_name` as an attribute to the class.

        MODIFICATIONS
        -------------
        Created : 5/8/19
        """
        if self.update_method == "PK":
            return self.sync_tbls_dict[tbl_name]
        elif self.update_method == "TS":
            if tbl_name not in ("CurrencyPairExchangeRate", "AltCurrencyPairExchangeRate"):
                return "timePeriodStartTs"
            else:
                return "currencyPairExchangeRateTs"

    def create_current_pk_df(self):
        """
        DESCRIPTION
        -----------
        Query each database table in the sync tables dictionary and obtain the 
        last available primary key value.  Create a dataframe which stores this 
        data with the table name as the index of the frame.

        NOTES
        -----
        Assigns curr_update_df as an attribute of the class.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        self.curr_update_df = pd.DataFrame()
        for tbl, prim_key in self.sync_tbls_dict.items():
            self.update_current_pk_df(tbl, prim_key)

    def update_current_pk_df(self, tbl_name, prim_key):
        """
        DESCRIPTION
        -----------
        Retrieve and replace the current update value for a table given its 
        primary key and table name.

        PARAMETERS
        ----------
        tbl_name : str  
            A string value representing the name of the table in the database.
        prim_key : str
            A string value representing the name of the primary key field in the 
            given table.

        NOTES
        -----
        Modifies the curr_update_df in place.

        MODIFICATIONS
        -------------
        Created : 5/8/19
        """
        last_pk = self.get_last_pk(tbl_name, prim_key)
        self.curr_update_df.loc[tbl_name, self.comp_field_name] = last_pk.values 

    def create_current_ts_df(self):
        """
        DESCRIPTION
        -----------
        Query each database table in the sync tables dictionary and obtain the 
        last available timestamp value.  Create a dataframe which stores this 
        data with the table name as the index of the frame.

        NOTES
        -----
        Assigns curr_update_df as an attribute of the class.

        MODIFICATIONS
        -------------
        Created ; 5/7/19
        """
        self.curr_update_df = pd.DataFrame()
        for tbl in self.sync_tbls_dict:
            self.update_current_ts_df(tbl)

    def update_current_ts_df(self, tbl_name):
        """
        DESCRIPTION
        -----------
        Retrieve and replace the current update timestamp value for a table given 
        the name of the table.

        PARAMETERS
        ----------
        tbl_name : str  
            A string value representing the name of the table in the database.

        NOTES
        -----
        Modifies the curr_update_df in place.

        MODIFICATIONS
        -------------
        Created : 5/8/19
        """
        last_ts = self.get_last_update_ts(tbl_name)
        self.curr_update_df.loc[tbl_name, self.comp_field_name] = last_ts.values

    def eval_if_tbls_have_been_modified(self):
        """
        DESCRIPTION
        -----------
        Evaluate using a boolean flag whether any database table has been modified
        since the last save of the sync module.

        RETURNS
        -------
        Boolean value representing whether any table has been modified since the
        last save of the sync module.

        NOTES
        -----
        This is useful for checking a database with itself, it is not meant to 
        evaluate whether the master and slave databases are synced.

        MODIFICATIONS
        -------------
        Created : 5/6/19
        """
        return not self.curr_update_df.equals(self.last_update_df)

    def create_pk_csv_locations(self):
        """
        DESCRIPTION
        -----------
        Build the filepath for the existing primary key .csv file as well as the 
        backup filepath.

        NOTES
        -----
        Assigns the filepath strings to the DB class instance.

        MODIFICATIONS
        -------------
        Created : 5/6/19
        """
        self.pk_csv_fp = f"{os.getcwd()}/src/stream/sync/pk_saves/{self.serv_name}.csv"
        self.pk_csv_backup_fp = f"{os.getcwd()}/src/stream/sync/backup_pk_saves/{self.serv_name}.csv"

    def create_ts_csv_locations(self):
        """
        DESCRIPTION
        -----------
        Build the filepath for the existing last timestamp update .csv file as well
        as the backup filepath.

        NOTES
        -----
        Assigns the filepath strings to the DB class instance.

        MODIFICATIONS
        -------------
        Created : 5/6/19
        """
        self.ts_csv_fp = f"{os.getcwd()}/src/stream/sync/ts_saves/{self.serv_name}.csv"
        self.ts_csv_backup_fp = f"{os.getcwd()}/src/stream/sync/backup_ts_saves/{self.serv_name}.csv"

    def initial_sync_actions(self):
        """
        DESCRIPTION
        -----------
        A set of actions taken before a comparison between master and slave databases
        are even made.  If any table has been modified since the last saved sync
        location and the database is not master and the overwrite parameters is 
        True, prepare the table for sync by deleting rows occurring after the last
        save sync location or truncating the table completely if the last save 
        location does not exist (first time sync).

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        if (self.are_tbls_modified) & (not self.master_db) & (self.overwrite_slave): # DO NOT MODIFY MASTER DB
            if self.last_update_df is None:
                for tbl in self.sync_tbls_dict:
                    self.truncate_table(tbl)
            else:
                for tbl in self.curr_update_df.index:
                    if not tbl in self.last_update_df.index:
                        self.truncate_table(tbl)
                    else:
                        if self.update_method == "PK":
                            self.delete_by_last_pk_id(
                                tbl, 
                                self.sync_tbls_dict[tbl],
                                self.last_update_df.loc[tbl, self.comp_field_name]
                            )
                        elif self.update_method == "TS":
                            self.delete_by_timeperiod_start_ts(
                                tbl,
                                self.last_update_df.loc[tbl, self.comp_field_name]
                            )

    def save_updates_to_csv(self, df, fp, backup_fp):
        """
        DESCRIPTION
        -----------
        Save the current updates containing primary key or latest timestamp to 
        file after a database sync from master to slave is completed.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance containing primary key or latest timestamp
            values with the database table name as the table's index.
        fp : str 
            A string value representing the filepath for which to save the .csv file.
        backup_fp : str 
            A string value representing the backup filepath for which to save the 
            .csv file in case the first filepath is lost or corrupted.

        NOTES
        -----
        The "w" mode on to_csv overwrites any file currently in place.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        df.to_csv(fp, mode="w")
        df.to_csv(backup_fp, mode="w")

    def read_from_last_pk_id(self, tbl_name, prim_key, last_value, limit=50000):
        """
        DESCRIPTION
        -----------
        Query all rows of a table whose primary key value is greater than the 
        last saved value.  Return as a pandas dataframe instance.

        PARAMETERS
        ----------
        tbl_name : str
            A string value representing the name of the database table 
        prim_key : str 
            A string value representing the field name of the primary key in the 
            database table.
        last_value : int, float
            An integer or float value (should be integer but float should be 
            supported) representing the last saved value of the primary key from 
            the last sync.
        limit : int (default=50000)
            An integer value representing the maximum number of rows to download
            in a single query.

        RETURNS
        -------
        A pandas dataframe instance containing rows whose primary key value is 
        greater than the last_value parameter.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        query = ("SELECT "
                        "* " 
                     "FROM "
                        f"{tbl_name} "
                     "WHERE "
                        f"{prim_key} > {last_value} "
                     f"ORDER BY {prim_key} "
                     f"LIMIT {limit};" 
        )
        return self.select(query)

    def read_from_last_timeperiod_start_ts(self, tbl_name, last_value, limit=50000):
        """
        DESCRIPTION
        -----------
        Query all rows of a table where the timePeriodStartTs is greater than the 
        last saved value.  Return these rows as a pandas dataframe instance.

        PARAMETERS
        ----------
        tbl_name : str 
            A string value representing the name of the table in the database.
        last_value : int, float
            An integer or float value (should be integer but float should be 
            supported) representing the last saved value of the timePeriodStartTs from 
            the last sync.
        limit : int (default=50000)
            An integer value representing the maximum number of rows to download
            in a single query.

        RETURNS
        -------
        A pandas dataframe instance containing rows whose timePeriodStartTs value
        is greater than the last saved parameter.

        MODIFICATIONS
        -------------
        Created ; 5/7/19
        """
        if tbl_name not in ("CurrencyPairExchangeRate", "AltCurrencyPairExchangeRate"):
            query = ("SELECT "
                      "* "
                     "FROM "
                        f"{tbl_name} tbl "
                     "JOIN "
                        "TimePeriodDimension tpd "
                        "ON "
                        f"{tbl_name}.timePeriodDimensionId = tpd.timePeriodDimensionId "
                     "WHERE "
                        f"tpd.timePeriodStartTs > '{last_value}' "
                     "ORDER BY tpd.timePeriodStartTs "
                     f"LIMIT {limit};"
            )
        else:
            query = ("SELECT "
                        "* "
                     "FROM "
                        f"{tbl_name} "
                     "WHERE "
                        "currencyPairExchangeRateTs > '{last_value}' "
                     "ORDER BY currencyPairExchangeRateTs "
                     "LIMIT {limit};"
            )
        return self.select(query)

    def read_from_table(self, tbl_name, last_value, limit=50000):
        """
        DESCRIPTION
        -----------
        A wrapper function used to return a pandas dataframe containing all rows
        of a database table where the primary key or timestamp value  is greater 
        than the last_value parameter.

        PARAMETERS
        ----------
        tbl_name : str
            A string value representing the name of the database table.
        last_value : int, float
            An integer or float value (should be integer but float should be 
            supported) representing the last saved value of the primary key from 
            the last sync.
        limit : int (default=50000)
            An integer value representing the maximum number of rows to download
            in a single query.

        RETURNS
        -------
        A pandas dataframe instance containing all rows of a database table where 
        the primary key or timestamp is greater than the last value provided.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        if self.update_method == "PK":
            prim_key = self.sync_tbls_dict[tbl_name]
            return self.read_from_last_pk_id(tbl_name, prim_key, last_value, limit=limit)
        elif self.update_method == "TS":
            return self.read_from_last_timeperiod_start_ts(tbl_name, last_value, limit=limit)
        else:
            raise ValueError(f"{self.update_method} is not a supported update method.")

    def truncate_table(self, tbl_name):
        """
        DESCRIPTION
        -----------
        Truncate a database table.

        PARAMETERS
        ----------
        tbl_name : str 
            A string value representing the name of the database table.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        query = "TRUNCATE TABLE {tbl_name};"
        self.update(query)
        self.sync_log.info(f"{self.serv_name} {tbl_name} table truncated.")
        

    def delete_by_last_pk_id(self, tbl_name, pk_col, del_after_id):
        """
        DESCRIPTION
        -----------
        Create and execute a DELETE SQL query to delete all rows after a provided
        primary key id value from a provided database table.

        PARAMETERS
        ----------
        tbl_name : str 
            A string value representing a database table name.
        pk_col : str 
            A string value representing the name of the primary key value in the table.
        del_after_id : int, float
            An integer or float value (should be integer but float should be 
            supported) representing the last saved value of the primary key from 
            the last sync, after which all new rows should be deleted.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        query = ("DELETE "
                    f"{tbl_name} "
                "FROM "
                    f"{tbl_name} "
                "WHERE "
                    f"{tbl_name}.{pk_col} > {del_after_id};"
        )
        self.update(query)
        self.sync_log.info(f"{self.serv_name} {tbl_name} PK entries deleted.")

    def delete_by_timeperiod_start_ts(self, tbl_name, del_after_ts):
        """
        DESCRIPTION
        -----------
        Create and execute a DELETE SQL query to delete all rows after a provided
        timestamp from a provided database table.

        PARAMETERS
        ----------
        tbl_name : str
            A string value representing a database table name.
        del_after_ts : dt.datetime
            A datetime or timestamp object representing the timestamp to be used 
            as a condition to delete any future rows after.

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        if tbl_name not in ("CurrencyPairExchangeRate", "AltCurrencyPairExchangeRate"):
            query = ("DELETE "
                        f"{tbl_name} "
                     "FROM "
                        f"{tbl_name} "
                     "JOIN TimePeriodDimension "
                        f"ON TimePeriodDimension.timePeriodDimensionId = {tbl_name}.timePeriodDimensionId "
                     "WHERE "
                        f"TimePeriodDimension.timePeriodStartTs > '{del_after_ts}';"
            )
        else:
            query = ("DELETE "
                     "FROM "
                     f"{tbl_name} "
                     "WHERE "
                        f"currencyPairExchangeRateTs > '{del_after_ts};"
            )
        self.update_query(query)
        self.sync_log.info(f"{self.serv_name} {tbl_name} TS entries deleted after {del_after_ts}")
        

    def read_last_saved_csv(self, fp, backup_fp):
        """
        DESCRIPTION
        -----------
        Attempt to read the last primary key values from a written .csv file stored
        at the `pk_csv_fp` filepath.  If the filepath does not exist, attempt to 
        read from the backup file.  If this also does not exist, return a Null value.
        
        PARAMETERS
        ----------
        fp : str
            A string value representing the expected location of the file 
            containing the last updates made from a database sync broken down 
            by table name.
        backup_fp : str
            A string value representing the expected location of the backup file
            containing the last updates made from a database sync broken down by 
            table name in case original filepath is for some reason moved or 
            deleted.

        RETURNS
        -------
        A pandas dataframe instance containing the last inserted primary key values
        of a database sync or Null if the .csv file does not exist.

        MODIFICATIONS
        -------------
        Created ; 5/6/19
        """
        try:
            return pd.read_csv(fp, index_col=0)
        except FileNotFoundError as orig_nfe:
            try:
                return pd.read_csv(backup_fp, index_col=0)
            except FileNotFoundError as backup_nfe:
                return None 

class Sync(object):
    """
    DESCRIPTION
    -----------
    A class designed to create multiple database instances and sync tables across
    databases provided one server is named master server.

    PARAMETERS
    ----------
    top_event : threading.Event
        An event flag which will be used to inform any sync threads of an outside
        event such as a keyboard interrupt terminating the program.
    master_db_name : str
        A string value representing the name of the server containing the master
        database.
    sync_serv_lst: list, NoneType (default=None)
        A list containing the names of the servers which will be evaluated and synced.
        If None is passed, a default server list (All Servers) will be passed.
    sync_tbls_lst : list, NoneType (default=None)
        A list containing the names of the tables which will be evaluated and synced.
        If None is passed, a default list of tables (All Tables) will be passed.
    remote_sync : bool (default=False)
        A boolean flag representing whether the database is being accessed via 
        SSH tunnel from a remote location.
    use_internal : bool (default=False)
        A boolean flag representing whether to use internal IP addresses (in case
        where all servers can be accessed within the same network) 
    update_method : str (default="PK")
        A string value representing the method for which to track current and last
        saved locations for database tables.  Currently supported options include 
        PK (primary key) and TS (timestamp).
    overwrite_slave : bool (default=False)
        A boolean flag representing whether to overwrite any modifications made to 
        a slave database from the last save point by syncing from the master.

    MODIFICATIONS
    -------------
    Created : 5/7/19
    """
    def __init__(self, top_event, master_db_name, sync_serv_lst=None, 
                 sync_tbls_lst=None, remote_sync=False, use_internal=False,
                 update_method="PK", overwrite_slave=False):
        self.top_event = top_event 
        self.master_db_name = master_db_name 
        self.sync_serv_lst = sync_serv_lst 
        self.sync_tbls_lst = sync_tbls_lst 
        self.overwrite_slave = overwrite_slave
        self.remote_sync = remote_sync 
        self.use_internal = use_internal
        self.update_method = update_method  
        self.initialize_sync()

    def initialize_sync(self):
        """
        DESCRIPTION
        -----------
        A wrapper function which calls for the establishment of a logging instance,
        verifies provided server names, and creates instances of the DB class 
        for each server provided.

        MODIFICATIONS
        -------------
        Created ; 5/6/19
        """
        self.establish_log_instance()
        self.create_default_sync_server_list()
        self.verify_sync_serv_lst()
        self.create_db_instances()
        self.sync_event = Event()
        self.alert_event = Event()

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
        Modified : 5/3/19
            - Adapted for the sync class from the base strategy class.
        """
        self.sync_log = logging.getLogger("sync")
        self.sync_log.info("Sync logger has been established.")   

    def create_default_sync_server_list(self):
        """
        DESCRIPTION
        -----------
        Assign the default slave server list to sync all data from master

        NOTES
        -----
        Assigns the default sync server list to the class.

        MODIFICATIONS
        -------------
        Created : 5/6/19
        """
        self.default_sync_serv_lst = ["DEV_1", "DEV_2", "DEV_3", "DEV_4", "LIVE_1"]

    def verify_sync_serv_lst(self):
        """
        DESCRIPTION
        -----------
        Compare the user input server list to the default server list to verify 
        that all user entries are valid server names.

        RAISES
        ------
        NameError : 
            In the case that the server name provided does not match any of the 
            default server names, raise this error to ensure the user input is 
            corrected.
        TypeError : 
            In the case that neither a list or the default None argument is passed
            by the user, raise this error to inform that the input type provided
            is not valid.

        MODIFICATIONS
        -------------
        Created : 5/6/19
        Modified : 5/8/19
            - Added a conditional for including the master db in the case where the
              master db name specified was not included in the sync server list.
        """
        if self.sync_serv_lst is None:
            self.sync_serv_lst = self.default_sync_serv_lst
        elif isinstance(self.sync_serv_lst, list):
            if not self.master_db_name in self.sync_serv_lst:
                self.sync_serv_lst.append(self.master_db_name)
            for serv_name in self.sync_serv_lst:
                if serv_name not in self.default_sync_serv_lst:
                    raise NameError(f"{serv_name} is not a supported server name.")
        else:
            raise TypeError(f"sync server list must be Nonetype or of type `list`")

    def create_db_instances(self):
        """
        DESCRIPTION
        -----------
        Create instances of the DB class for connecting to, querying, and updating
        tables on the MySQL database associated with each class from the designated
        master server to the connected database.

        NOTES
        -----
        Instances of the DB class are stored in db_instances, a dictionary which 
        will be used to make sync updates in a easy to iterate through format.

        MODIFICATIONS
        -------------
        Created ; 5/6/19
        """
        self.db_instances = {}
        for serv_name in self.sync_serv_lst:
            master_db = True if serv_name == self.master_db_name else False 
            self.db_instances[serv_name] = DB(
                    serv_name,
                    self.sync_tbls_lst,
                    self.sync_log, 
                    master_db=master_db, 
                    use_internal=self.use_internal, 
                    remote_sync=self.remote_sync,
                    update_method=self.update_method,
                    overwrite_slave=self.overwrite_slave
            )

    def compare_with_master(self, master_update_df, slave_update_df, tbl, col):
        """
        DESCRIPTION
        -----------
        Return a boolean value representing whether the last update value of the 
        master database table is ahead of the slave database.

        PARAMETERS
        ----------
        master_update_df : pd.DataFrame
            A pandas dataframe instance containing the last update value of the 
            master database with the list of table names as the index of the frame.
        slave_update_df : pd.DataFrame 
            A pandas dataframe instance containing the last update value of the 
            slave database with the list of table names as the index of the frame.
        tbl : str 
            A string value representing the name of the table in the database.
        col : str
            A string value representing the name of the field for comparison.

        RETURNS
        -------
        A boolean value representing whether the master table is ahead of the 
        slave table 

        MODIFICATIONS
        -------------
        Created : 5/7/19
        """
        return master_update_df.loc[tbl, col] > slave_update_df.loc[tbl, col]

    def get_save_filepaths(self):
        """
        DESCRIPTION
        -----------
        Retrieve the filepaths for csv files saving last primary key or timestamp
        update information.

        RAISES
        ------
        ValueError : 
            Raise a value error if the update method is not a supported method.

        MODIFICATIONS
        -------------
        Created : 5/9/19
        """
        if self.update_method == "PK":
            return slave_inst.pk_csv_fp, slave_inst.pk_csv_backup_fp
        elif self.update_method == "TS":
            return slave_inst.ts_csv_fp, slave_inst.ts_csv_backup_fp
        else:
            raise ValueError(f"{self.update_method} is not a supported update method.")

    def sync_actions(self, serv_name, mdb_inst, slave_inst):
        """
        DESCRIPTION
        -----------
        A function which syncs all of the given tables from the master database
        instance to the slave database instance. The server name given is that 
        of the slave which we are writing to.

        PARAMETERS
        ----------
        serv_name : str 
            A string value representing the name of the server which contains the
            slave database being written to.
        mdb_inst : DB
            A database instance for the master database.
        slave_inst : DB
            A database instance for the slave database.

        MODIFICATIONS
        -------------
        Created ; 5/9/19
        """
        try:
            slave_inst.update_db_table_positions()  
            for tbl in mdb_inst.curr_update_df.index:
                needs_update = self.compare_with_master(
                    mdb_inst.curr_update_df, 
                    slave_inst.curr_update_df,
                    tbl,
                    mdb_inst.comp_field_name 
                )
                if needs_update:
                    self.sync_loop(mdb_inst, slave_inst, tbl)
        except Exception as ex:
            self.general_exception_actions(serv_name)

    def general_exception_actions(self, serv_name):
        """
        DESCRIPTION
        -----------
        A function which acts as an exception handler as a catch all for any 
        exception which might occur during a database sync operation.

        MODIFICATIONS
        -------------
        Created : 5/9/19
        """
        exc_line_no = sys.exc_info()[-1].tb_lineno
        self.sync_log.exception(f"An exception occurred on line {exc_line_no}.")
        if not self.alert_event.is_set():
            GeneralSyncException(serv_name, exc_line_no)
            self.alert_event.set()

    def sync_loop(self, mdb_inst, slave_inst, tbl):
        """
        DESCRIPTION
        -----------
        The primary purpose of this loop is to iterate through large amounts of 
        data for each tbl sync.  This is optimal for when large data transfers
        are necessary, such as rebuilding tables from scratch or after a long 
        period of idle time.

        PARAMETERS
        ----------
        mdb_inst : DB
            A database instance for the master database.
        slave_inst : DB
            A database instance for the slave database.
        tbl : str 
            A string value representing the name of the table to sync.

        MODFICATIONS
        ------------
        Created : 5/9/19
        """
        while True:
            mdb_rows = mdb_inst.read_from_table(
                tbl, 
                slave_inst.curr_update_df.loc[tbl, slave_inst.comp_field_name]
            )
            mdb_rows = utils.set_null_vals_to_none(mdb_rows)
            col_name = slave_inst.get_db_field_name(tbl)
            slave_inst.write_df_in_chunks(mdb_rows, tbl)
            if self.update_method == "PK":
                slave_inst.update_current_pk_df(tbl, col_name)
            elif self.update_method == "TS":
                slave_inst.update_current_ts_df(tbl)
            else:
                raise ValueError(f"{self.update_method} is not a supported update method.")
            slave_update_val = slave_inst.curr_update_df.loc[tbl, slave_inst.comp_field_name]
            mdb_update_val = mdb_inst.curr_update_df.loc[tbl, mdb_inst.comp_field_name]
            if slave_update_val >= mdb_update_val:
                fp, backup_fp = self.get_save_filepaths()
                slave_inst.save_updates_to_csv(
                    slave_inst.curr_update_df,
                    fp,
                    backup_fp
                )
                self.sync_log(
                    f"{tbl} has been synced from {mdb_inst.serv_name} to {slave_inst.serv_name} "
                )
                break 

    def open_sync_thread(self, serv_name, mdb_inst, slave_inst, wait_secs):
        """
        DESCRIPTION
        -----------
        Open a thread to perform the entirety of the syncing process between the 
        master database and a slave database.  A thread will be opened for each 
        slave database.

        PARAMETERS
        ----------
        serv_name : str 
            A string value representing the name of the server on which the slave
            database is contained.
        mdb_inst : DB
            A DB instance for the master database.
        slave_inst : DB
            A DB instnace for the slave database.
        wait_secs : int 
            An integer value representing the number of seconds until the the next
            sync iteration.

        MODIFICATIONS
        -------------
        Created : 5/9/19
        """
        t = threading.Thread(name=f"{serv_name}_sync_thread",
                             target=self.run, 
                             kwargs={
                                "serv_name":serv_name,
                                "mdb_inst":mdb_inst, 
                                "slave_inst":slave_inst,
                                "wait_secs":wait_secs
                             }
        ) # open thread for each necessary level timeperiod type
        t.setDaemon(True)
        t.start()
        self.sync_log.info(f"{t.name} has opened.")

    def wait_loop(self, wait_secs):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether any flags due to exceptions or manual 
        keyboard interrupts have been triggered, else activating a sleep period
        until triggered by the time elapsed greater than the necessary wait period.

        MODIFICATIONS
        -------------
        Created : 5/8/19
        """
        start_wait_time = time.time()
        while True:
            if self.top_event.is_set():
                break
            elif self.sync_event.is_set():
                break  
            elif time.time() - start_wait_time > wait_secs:
                break 
            else:
                self.sync_event.wait(0.1)

    def break_explanation(self):
        """
        DESCRIPTION
        -----------
        Create a log message detailing the reasoning for a break in a sync thread, 
        whether it be a global keyboard interrupt (top event set) or an exception 
        hit (sync event set).

        MODIFICATIONS
        -------------
        Created : 5/8/19
        """
        msg = "An unexplained error "
        if self.top_event.is_set():
            msg = "The top event has been flagged and "
        elif self.sync_event.is_set():
            msg = "The sync event has been flagged and "
        self.sync_log.info(msg + f"has caused the thread to close.")

    def main(self):
        """
        DESCRIPTION
        -----------
`       A wrapper function which creates threads for each slave server instance
        to be synced.

        PARAMETERS
        ----------
        wait_secs : int (default=300)
            An integer value representing the number of seconds to wait as an 
            interval between sync updates.

        MODIFICATIONS
        -------------
        Created : 5/9/19
        """
        mdb_inst = self.db_instances[self.master_db_name]
        mdb_inst.update_db_table_positions()
        for serv_name, slave_inst in self.db_instances.items():
            # create thread here for each server 
            if serv_name == self.master_db_name:
                continue
            self.run(serv_name, mdb_inst, slave_inst, wait_secs=300)
            # self.open_sync_thread(serv_name, mdb_inst, slave_inst)

    def run(self, serv_name, mdb_inst, slave_inst, wait_secs=300):
        """
        DESCRIPTION
        -----------
        A loop which evaluates whether an external or sync event has occurred.
        If False, establishes threads to sync all databases in the main function.
        Then sleeps until the check is made.

        PARAMETERS
        ----------
        wait_secs : int (default=300)
            An integer value representing the number of seconds to wait between
            database syncs.

        MODIFICATIONS
        -------------
        Created : 5/9/19
        """
        while True:
            top_level_event_cond = not self.top_event.is_set()
            sync_event_cond = not self.sync_event.is_set()
            if not (top_level_event_cond & sync_event_cond):
                self.break_explanation()
                break
            self.sync_actions(serv_name, mdb_inst, slave_inst)
            self.wait_loop(wait_secs=wait_secs)
            

if __name__ == "__main__":
    top_event = Event()
    sync = Sync(
        top_event, 
        master_db_name="DEV_2", 
        sync_serv_lst=["DEV_3"],
        overwrite_slave=True,  
        remote_sync=True, 
        update_method="PK"
    )
    sync.main()
    
    
import sys
import datetime as dt  

class BaseHistoricalBars(object):
    """
    DESCRIPTION
    -----------
    A class that serves as the base class for all historical bar streaming and 
    resampling.

    MODIFICATIONS
    -------------
    Created : 5/1/19
    """
    def __init__(self, BaseStreaming, top_event, use_utc=False):
        self.base = BaseStreaming
        self.top_event = top_event
        self.use_utc = use_utc

    @staticmethod
    def add_curr_pair_id(df, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Add a currency pair id field to the pandas dataframe.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance
        curr_pair_id : int
            An integer value representing the associated currency pair in the database.

        NOTES
        -----
        Modifies the dataframe by adding the column in place.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        df["currencyPairId"] = curr_pair_id

    def get_vendor_id(self, vendor_cd):
        """
        DESCRIPTION
        -----------
        Assign vendor id based on the vendor dictionary value for Dukascopy in the 
        base inheritance class.

        PARAMETERS
        ----------
        vendor_cd : str 
            A string value representing the data stream vendor.

        RETURNS
        -------
        An integer value representing the associated id for the vendor pertaining
        to the database.

        MODIFICATIONS
        -------------
        Created : 4/28/19
        Modified : 5/1/19
            - Added parameter `vendor_cd` to apply to all vendors.
            - Changed func name from get_dk_vendor_id to get_vendor_id.
            - Transferred func from DukascopyHistoricalBars to BaseHistoricalBars
        """
        return self.base.vendor_dict[vendor_cd]

    @staticmethod
    def add_vendor_id(df, vendor_id):
        """
        DESCRIPTION
        -----------
        Assign the vendor id field to a pandas dataframe.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance

        NOTES
        -----
        Modifies the dataframe in place

        MODIFICATIONS
        -------------
        Created ; 4/29/19
        """
        df["vendorId"] = vendor_id

    @staticmethod
    def add_available_ind(df):
        """
        DESCRIPTION
        -----------
        Append a field to a pandas dataframe as a boolean flag telling whether 
        aggregated currency pair exchange rate data is available at the row index
        value.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance

        NOTES
        -----
        Modifies the dataframe by adding the available index column in place.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        df.loc[df.currencyPairExchangeRateInitial.isnull(), "currencyPairExchangeRateAvailableInd"] = 0
        df.loc[df.currencyPairExchangeRateInitial.notnull(), "currencyPairExchangeRateAvailableInd"] = 1

    def get_last_update_ts(self, agg_tbl_name, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Create, format, and execute an SQL query to find the latest timestamp
        available in the database for a currency pair in a specified aggregated
        currency pair exchange rate data table.

        PARAMETERS
        ----------
        agg_tbl_name : str 
            A string value representing the associated table name from which to query
            the database.
        curr_pair_id : int
            An integer value representing the associated currency pair in the database.

        RETURNS
        -------
        A datetime object representing the most up-to-date timestamp of aggregated
        exchange rate data available in the database for a given currency pair.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        Modified : 4/30/19
            - Removed tbl_name as a parameter in favor of "hardcoding" the agg_tbl_name
              assigned to the class instance.
        Modified : 5/1/19
            - Re-added the agg_tbl_name parameter so as to accomodate the base class
              for access to all tables rather than specifically for the Dukascopy 
              1-minute table.
        """
        query = ("SELECT "
                    "tpd.timePeriodStartTs "
                 "FROM "
                     f"{agg_tbl_name} cper "
                         "JOIN "
                     "TimePeriodDimension tpd ON cper.timePeriodDimensionId = tpd.timePeriodDimensionId "
                 "WHERE "
                    f"cper.currencyPairId = {curr_pair_id} "
                 "ORDER BY tpd.timePeriodStartTs DESC "
                 "LIMIT 1;"
        )
        last_update_ts_df = self.base.select(query) # db ts is a naive ts
        if last_update_ts_df.empty:
            return dt.datetime(2007, 6, 1, 0, 0, tz_info=self.base.utc if self.use_utc else self.base.est)
        else:
            return last_update_ts_df.loc[0, "timePeriodStartTs"].replace(tzinfo=self.base.utc if self.use_utc else self.base.est)

    def get_avail_data_in_period(self, agg_tbl_name, beg_date, end_date, curr_pair_id):
        """
        DESCRIPTION
        -----------
        Create, format, and execute an SQL query to determine all available aggregated
        exchange rate data available in the database table adhering to a begin 
        and end date range and a specified currency pair.

        PARAMETERS
        ----------
        beg_date : dt.datetime
            A datetime or timestamp like object representing the beginning date 
            to query existing aggregated exchange rate data.
        end_date : dt.datetime
            A datetime or timestamp like object representing the ending date to 
            query existing aggregated exchange rate data.
        curr_pair_id : int 
            An integer value representing the associated currency pair in the 
            database.

        RETURNS
        -------
        A pandas dataframe instance containing all existing aggregated exchange
        rate data within a specified date range and with a specified currency pair
        found in the database. 

        MODIFICATIONS
        -------------
        Created ; 4/29/19
        Modified : 4/30/19
            - Removed tbl_name as a parameter in favor of "hardcoding" as the agg_tbl_name
              assigned to the class instance.
        """
        query = ("SELECT "
                    "tpd.timePeriodDimensionId, "
                    "tpd.timePeriodStartTs, "
                    "cper.currencyPairExchangeRateInitial, "
                    "cper.currencyPairExchangeRateMax, "
                    "cper.currencyPairExchangeRateMin, "
                    "cper.currencyPairExchangeRateFinal, "
                    "cper.currencyPairId, "
                    "cper.vendorId, "
                    "cper.currencyPairExchangeRateAvailableInd "
                 "FROM "
                    f"{agg_tbl_name} cper "
                    "JOIN "
                        "TimePeriodDimension tpd ON cper.timePeriodDimensionId = tpd.timePeriodDimensionId "
                  "WHERE "
                    f"tpd.timePeriodStartTs >= '{beg_date}' "
                    f"AND tpd.timePeriodStartTs < '{end_date}' "
                    f"AND cper.currencyPairId = {curr_pair_id} "
                    f"AND tpd.timePeriodHasOperationalMarketInd = 1;"
        )
        return self.base.select(query)

    def get_avail_timeperiod_dimensions(self, beg_date, end_date, tpt_id=1):
        """
        DESCRIPTION
        -----------
        Get a pandas dataframe containing all available timeperiod dimensions 
        over a specified date range given a begin and end date and a dimension type.

        PARAMETERS
        ----------
        beg_date : dt.datetime
            A datetime or timestamp object representing the beginning of an interval
            range.
        end_date : dt.datetime
            A datetime or timestamp object representing the ending of an interval
            range. 
        tpt_id : int (default=1)
            An integer representing the timeperiod type id association created in 
            the database.

        RETURNS
        -------
        A pandas dataframe instance containing all available timeperiod dimensions
        returned from the SQL query adhering to the begin and end date range with 
        the correct associated timeperiod type.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        query = ("SELECT "
                    "timePeriodDimensionId, "
                    "timePeriodStartTs "
                 "FROM "
                    "TimePeriodDimension "
                 "WHERE "
                    f"timePeriodStartTs >= '{beg_date}' "
                    f"AND timePeriodEndTs < '{end_date}' "
                    f"AND timePeriodTypeId = {tpt_id} "
                    "AND timePeriodHasOperationalMarketInd = 1;"
        )
        return self.base.select(query)

    @staticmethod
    def filter_curr_pair_avail_rows(df):
        """
        DESCRIPTION
        -----------
        Filter a pandas dataframe instance and return only rows where data is 
        not null.

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance

        RETURNS
        -------
        A pandas dataframe copy containing rows where data is not null.

        MODIFICATIONS
        -------------
        Created ; 4/29/19
        """
        return df[df.currencyPairExchangeRateAvailableInd.notnull()]

    def format_datetime(self, df):
        """
        DESCRIPTION
        -----------
        Convert a timezone series first to the desired timezone then to a naive
        timestamp to pass as a readable MySQL format

        PARAMETERS
        ----------
        df : pd.DataFrame
            A pandas dataframe instance containing a timePeriodStartTs column

        NOTES
        -----
        Modifies the existing pandas dataframe in place.

        MODIFICATIONS
        -------------
        Created : 4/29/19
        """
        tz = self.base.utc if self.use_utc == True else self.base.est 
        df["timePeriodStartTs"] = df["timePeriodStartTs"].dt.tz_convert(tz).dt.tz_localize(None)



import pandas as pd
import requests
import pytz 

from datetime import timedelta as td 
from dateutil.relativedelta import relativedelta as rd
"""
DESCRIPTION
-----------
Utils used for all streaming applications.

MODIFICATIONS
-------------
Created : 4/24/19

AUTHORS
-------
CJ
"""
def reverse_key_value(orig_dict):
    """
    DESCRIPTION
    -----------
    Reverse the key value pairs of a dictionary object.

    PARAMETERS
    ----------
    orig_dict : dict 
        A dictionary object.

    RETURNS
    -------
    rev_dict : dict 
        A dictionary with the values of the original dictionary stored as keys
        and the keys of the oriinal dictionary stored as values.

    MODIFICATIONS
    -------------
    Created : 4/24/19
    """ 
    rev_dict = {}
    for j, k in orig_dict.items():
        rev_dict[k] = j 
    return rev_dict

def strip_tzinfo(ts):
    """ 
    DESCRIPTION
    -----------
    Strip the timezone info from a timestamp instance

    RETURNS
    -------
    A timestamp object with nullified timezone information.

    MODIFICATIONS
    -------------
    Created : 4/22/19
    """
    return ts.replace(tzinfo=None)

def round_down_to_nearest_day(ts):
    """
    DESCRIPTION
    -----------
    Round a timestamp to the nearest day by replacing hour, minute, second,
    and microsecond values to 0.

    RETURNS
    -------
    A rounded datetime object

    MODIFICATIONS
    -------------
    Created : 4/22/19
    """
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)

def round_down_to_nearest_week(ts):
    """
    DESCRIPTION
    -----------
    Round a timestamp to the nearest day by replacing hour, minute, second,
    and microsecond values to 0.

    RETURNS
    -------
    A rounded datetime object

    MODIFICATIONS
    -------------
    Created : 4/22/19
    """
    return ts.replace(hour=0, minute=0, second=0, microsecond=0) - td(days=ts.weekday())

def round_down_to_nearest_month(ts):
    """
    DESCRIPTION
    -----------
    Round a timestamp to the nearest month by replacing day, hour, minute, second,
    and microsecond values to 0.

    RETURNS
    -------
    A rounded datetime object

    MODIFICATIONS
    -------------
    Created : 4/22/19
    """
    return ts.replace(day=0, hour=0, minute=0, second=0, microsecond=0)

def concat_rows(df1, df2):
    """
    DESCRIPTION
    -----------
    A basic concatenation of two pandas dataframes adding rows of df2 to df1 
    ignoring indicies and outer joining on columns.

    PARAMETERS
    ----------
    df1 : pd.DataFrame
        A pandas dataframe instance. 
    df2 : pd.DataFrame
        A pandas dataframe instance. 

    RETURNS
    -------
    A pandas dataframe containing the results of the outer-joined concatenation.

    MODFICATIONS
    ------------
    Created : 4/24/19
    """
    return pd.concat([df1, df2], axis=0, sort=True, ignore_index=True)

def drop_first_df_row(df):
    """
    DESCRIPTION
    -----------
    Drop the first row of a pandas dataframe in place.
    
    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance

    NOTES
    -----
    Drop first row in place.

    MODIFICATIONS
    -------------
    Created : 4/25/19
    """
    df.drop(df.index[:1], inplace=True)

def drop_last_df_row(df):
    """
    DESCRIPTION
    -----------
    Drop the last row of a pandas dataframe in place

    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance

    NOTES
    -----
    Drop last row in place.

    MODIFICATIONS
    -------------
    Created : 4/25/19
    """
    df.drop(df.index[-1:], inplace=True)

def drop_df_cols(df, col_names):
    """
    DESCRIPTION
    -----------
    Drop specified columns from a pandas dataframe given a list of the column names
    in place.

    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance
    col_names : list
        A list of column names (str) for which to drop from the dataframe

    NOTES
    -----
    Drop columns in place.

    MODIFICATIONS
    -------------
    Created : 4/26/19
    """
    df.drop(col_names, axis=1, inplace=True)

def set_null_vals_to_none(df):
    """
    DESCRIPTION
    -----------
    Set null values to None to be SQL readable upon database insertion.

    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance

    RETURNS
    -------
    A pandas dataframe instance in which null values are converted to `None`

    MODIFICATIONS
    -------------
    Created ; 4/26/19
    """
    return df.where(pd.notnull(df), None)

def copy_df(df):
    """
    DESCRIPTION
    -----------
    Deep copy a pandas dataframe.

    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance

    RETURNS
    -------
    A deep copy of a given pandas dataframe

    MODIFICATIONS
    -------------
    Created : 4/26/19
    """
    return df.copy(deep=True)

def make_request(url):
    """
    DESCRIPTION
    -----------
    Make a request to a given url and return a response

    PARAMETERS
    ----------
    url : str 
        A url formatted as a string, the location to submit a request

    RETURNS
    -------
    response : a request response instance

    MODIFICATIONS
    -------------
    Created ; 4/26/19
    """
    return requests.get(url, stream=True)

def set_index(df, ix_cols):
    """
    DESCRIPTION
    Set the index of a pandas dataframe from existing columns.
    If multiple columns are passed in the ix_cols list, a multi-index
    will be formed.

    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance
    ix_cols : list 
        A list of column names formatted as strings referencing existing
        columns in the dataframe for which to make a (multi) index.

    RETURNS
    -------
    Creates a (multi) index in place

    NOTES
    -----
    To set a datetime index, a column with a dtype datetime must be passed.

    MODIFICATIONS
    -------------
    Created : 4/26/19
    """
    df.set_index(ix_cols, inplace=True)

def reset_index(df, drop=True):
    """
    DESCRIPTION
    -----------
    Reset the index of a pandas dataframe to a standard monotonic index.

    PARAMETERS
    ----------
    df : pd.DataFrame
        A pandas dataframe instance

    drop : bool (default=True)
        A boolean flag for whether to drop the current index or return the index
        as a dataframe column.

    NOTES
    -----
    Modifies the dataframe in place.

    MODIFICATIONS
    -------------
    Created : 4/28/19
    """
    df.reset_index(drop=drop, inplace=True)

def merge(df1, df2, on=None, how="left"):
    """
    DESCRIPTION
    -----------
    Merge two pandas dataframes on a common field name or specified fields in 
    each frame.

    PARAMETERS
    ----------
    df1 : pd.DataFrame
        A pandas dataframe instance
    df2 : pd.DataFrame
        A pandas dataframe instance
    on : str, list
        A string or list representing the field(s) to join on.  If a list is passed,
        it is assumed multiple values are passed for separate fields.  Otherwise, 
        it is assumed that a common field name is present between the two dataframes.
    how : str
        The method of joining the two frames (left, right, outer, inner)

    RETURNS
    -------
    A pandas dataframe instance merged from two provided dataframes

    MODIFICATIONS
    -------------
    Created : 4/29/19
    """
    if isinstance(on, list):
        return pd.merge(df1, df2, left_on=on[0], right_on=on[1], how=how)
    elif isinstance(on, str):
        return pd.merge(df1, df2, on=on, how=how)
    else:
        raise ValueError(f"merge_df function expected a list or string object, got {type(on)}")

def convert_ts_to_utc(ts):
    """
    DESCRIPTION
    -----------
    Return a timestamp or datetime object converted to UTC timezone

    PARAMETERS
    ----------
    ts : dt.datetime
        A datetime or timestamp instance

    RETURNS
    -------
    A datetime or timestamp instance with added UTC tz_info

    MODIFICATIONS
    -------------
    Created : 4/29/19
    """
    return ts.astimezone(pytz.timezone("UTC"))


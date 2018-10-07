import multiprocessing as mp
import pandas as pd

def worker(arg, errors, dayfirst, yearfirst, utc, box, format,
                           exact, unit, infer_datetime_format, origin, cache):
    '''
    Worker function for Pandas read_csv method.
    :return: datetime converted Series or Dataframe object.
    '''
    return pd.to_datetime(arg, errors, dayfirst, yearfirst, utc, box, format,
                           exact, unit, infer_datetime_format, origin, cache)


def exec_mp(worker_input):
    '''

    :param worker_input: Nested list of arguments for each process worker fucntion
    :return: A list of datetime converted Series or Dataframe object.
    '''
    core_count = mp.cpu_count()
    p = mp.Pool(core_count)
    return p.starmap(worker, worker_input)

def col_result(results):
    '''
    :param results: list of datetime converted objects.
    :return: Concatenated Dataframe or Series object.
    '''
    return pd.concat([result for result in results])


def worker_input(arg, errors, dayfirst, yearfirst, utc, box, format, exact,
                          unit, infer_datetime_format, origin, cache, slice_list):
    '''
    Generates an argument list for worker functions.
    :return: Argument list for worker function.
    '''
    arg_list = []
    for i in slice_list:
        lower_bound = i[0]
        upper_bound = i[1]
        arg_list.append([arg[lower_bound:upper_bound]])
    for i in arg_list:
        i.extend((errors, dayfirst, yearfirst, utc, box, format, exact,
                          unit, infer_datetime_format, origin, cache))
    return arg_list


def slice_list(arg):
    '''
    :param arg: Series or Dataframe object
    :return: A list of indicies for slicing the Series or Dataframe object into approx equal ammounts.
    '''
    core_count = mp.cpu_count()
    step = arg.shape[0] // core_count
    inner_list = [step]

    for item in range(1, core_count - 1):
        inner_list.append(step + inner_list[-1])
    inner_list.insert(0, 0)
    inner_list.append(None)

    outer_list = []
    for i in range(len(inner_list) - 1):
        outer_list.append([inner_list[i], inner_list[i + 1]])

    return outer_list

def to_datetime(arg, errors='raise', dayfirst=False, yearfirst=False,
                utc=None, box=True, format=None, exact=True,
                unit=None, infer_datetime_format=False, origin='unix',
                cache=False):
    """
    FROM PANDAS DOCUMENTATION:

        Convert argument to datetime.

        Parameters
        ----------
        arg : integer, float, string, datetime, list, tuple, 1-d array, Series

            .. versionadded:: 0.18.1

               or DataFrame/dict-like

        errors : {'ignore', 'raise', 'coerce'}, default 'raise'

            - If 'raise', then invalid parsing will raise an exception
            - If 'coerce', then invalid parsing will be set as NaT
            - If 'ignore', then invalid parsing will return the input
        dayfirst : boolean, default False
            Specify a date parse order if `arg` is str or its list-likes.
            If True, parses dates with the day first, eg 10/11/12 is parsed as
            2012-11-10.
            Warning: dayfirst=True is not strict, but will prefer to parse
            with day first (this is a known bug, based on dateutil behavior).
        yearfirst : boolean, default False
            Specify a date parse order if `arg` is str or its list-likes.

            - If True parses dates with the year first, eg 10/11/12 is parsed as
              2010-11-12.
            - If both dayfirst and yearfirst are True, yearfirst is preceded (same
              as dateutil).

            Warning: yearfirst=True is not strict, but will prefer to parse
            with year first (this is a known bug, based on dateutil beahavior).

            .. versionadded:: 0.16.1

        utc : boolean, default None
            Return UTC DatetimeIndex if True (converting any tz-aware
            datetime.datetime objects as well).
        box : boolean, default True

            - If True returns a DatetimeIndex
            - If False returns ndarray of values.
        format : string, default None
            strftime to parse time, eg "%d/%m/%Y", note that "%f" will parse
            all the way up to nanoseconds.
        exact : boolean, True by default

            - If True, require an exact format match.
            - If False, allow the format to match anywhere in the target string.

        unit : string, default 'ns'
            unit of the arg (D,s,ms,us,ns) denote the unit, which is an
            integer or float number. This will be based off the origin.
            Example, with unit='ms' and origin='unix' (the default), this
            would calculate the number of milliseconds to the unix epoch start.
        infer_datetime_format : boolean, default False
            If True and no `format` is given, attempt to infer the format of the
            datetime strings, and if it can be inferred, switch to a faster
            method of parsing them. In some cases this can increase the parsing
            speed by ~5-10x.
        origin : scalar, default is 'unix'
            Define the reference date. The numeric values would be parsed as number
            of units (defined by `unit`) since this reference date.

            - If 'unix' (or POSIX) time; origin is set to 1970-01-01.
            - If 'julian', unit must be 'D', and origin is set to beginning of
              Julian Calendar. Julian day number 0 is assigned to the day starting
              at noon on January 1, 4713 BC.
            - If Timestamp convertible, origin is set to Timestamp identified by
              origin.

            .. versionadded:: 0.20.0
        cache : boolean, default False
            If True, use a cache of unique, converted dates to apply the datetime
            conversion. May produce sigificant speed-up when parsing duplicate date
            strings, especially ones with timezone offsets.

            .. versionadded:: 0.23.0

        Returns
        -------
        ret : datetime if parsing succeeded.
            Return type depends on input:

            - list-like: DatetimeIndex
            - Series: Series of datetime64 dtype
            - scalar: Timestamp

            In case when it is not possible to return designated types (e.g. when
            any element of input is before Timestamp.min or after Timestamp.max)
            return will have datetime.datetime type (or corresponding
            array/Series).

        Examples
        --------
        Assembling a datetime from multiple columns of a DataFrame. The keys can be
        common abbreviations like ['year', 'month', 'day', 'minute', 'second',
        'ms', 'us', 'ns']) or plurals of the same

        >>> df = pd.DataFrame({'year': [2015, 2016],
                               'month': [2, 3],
                               'day': [4, 5]})
        >>> pd.to_datetime(df)
        0   2015-02-04
        1   2016-03-05
        dtype: datetime64[ns]

        If a date does not meet the `timestamp limitations
        <http://pandas.pydata.org/pandas-docs/stable/timeseries.html
        #timeseries-timestamp-limits>`_, passing errors='ignore'
        will return the original input instead of raising any exception.

        Passing errors='coerce' will force an out-of-bounds date to NaT,
        in addition to forcing non-dates (or non-parseable dates) to NaT.

        >>> pd.to_datetime('13000101', format='%Y%m%d', errors='ignore')
        datetime.datetime(1300, 1, 1, 0, 0)
        >>> pd.to_datetime('13000101', format='%Y%m%d', errors='coerce')
        NaT

        Passing infer_datetime_format=True can often-times speedup a parsing
        if its not an ISO8601 format exactly, but in a regular format.

        >>> s = pd.Series(['3/11/2000', '3/12/2000', '3/13/2000']*1000)

        >>> s.head()
        0    3/11/2000
        1    3/12/2000
        2    3/13/2000
        3    3/11/2000
        4    3/12/2000
        dtype: object

        >>> %timeit pd.to_datetime(s,infer_datetime_format=True)
        100 loops, best of 3: 10.4 ms per loop

        >>> %timeit pd.to_datetime(s,infer_datetime_format=False)
        1 loop, best of 3: 471 ms per loop

        Using a unix epoch time

        >>> pd.to_datetime(1490195805, unit='s')
        Timestamp('2017-03-22 15:16:45')
        >>> pd.to_datetime(1490195805433502912, unit='ns')
        Timestamp('2017-03-22 15:16:45.433502912')

        .. warning:: For float arg, precision rounding might happen. To prevent
            unexpected behavior use a fixed-width exact type.

        Using a non-unix epoch origin

        >>> pd.to_datetime([1, 2, 3], unit='D',
                           origin=pd.Timestamp('1960-01-01'))
        0    1960-01-02
        1    1960-01-03
        2    1960-01-04

        See also
        --------
        pandas.DataFrame.astype : Cast argument to a specified dtype.
        pandas.to_timedelta : Convert argument to timedelta.
        '''

    slices = slice_list(arg)
    inputs = worker_input(arg, errors, dayfirst, yearfirst, utc, box, format, exact,
                          unit, infer_datetime_format, origin, cache, slices)
    results = exec_mp(inputs)
    return col_result(results)
    """
    slices = slice_list(arg)
    input = worker_input(arg, errors, dayfirst, yearfirst, utc, box, format, exact,
                          unit, infer_datetime_format, origin, cache, slices)
    results = exec_mp(input)
    return col_result(results)




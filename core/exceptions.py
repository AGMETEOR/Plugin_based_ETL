"""Custom exceptions"""


class StatusFileReadError(Exception):
    pass


class StatusFileWriteError(Exception):
    pass


class NoDataFoundException(Exception):
    """
    Raise this custom exception when a call is made to the API
    and no data is returnd. This can happen when all the data has been exhausted
    """
    pass

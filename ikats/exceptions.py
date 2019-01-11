class IkatsException(Exception):
    """
    Generic Exception to identify the ones coming from Ikats
    """
    pass


class IkatsClientError(IkatsException):
    """
    Occurs when the response belongs to 4XX
    """
    pass


class IkatsInputError(IkatsClientError):
    """
    Occurs when backend detected a problem in the passed parameters
    eg. HTTP_CODE 400
    """
    pass


class IkatsConflictError(IkatsClientError):
    """
    Occurs when a conflict is detected on backend side
    eg. HTTP_CODE 409
    """
    pass


class IkatsNotFoundError(IkatsClientError):
    """
    Occurs when the request to backend side returns no data
    eg. HTTP_CODE 404
    """
    pass


class IkatsServerError(IkatsException):
    """
    Occurs when the backend could not respond correctly
    eg. HTTP_CODE 500
    """
    pass

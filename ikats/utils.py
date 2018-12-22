def check_type(value, allowed_types, var_name="variable", raise_exception=True):
    """
    Raises TypeError or return False if value doesn't belong to the provided list

    :param value: value to check
    :param allowed_types: list of allowed types, can be directly set to the type if only one is allowed
    :param var_name: name of the variable for the message
    :param raise_exception: indicates if an exception shall be raised in case of error

    :type value: any
    :type allowed_types: any
    :type var_name: str
    :type raise_exception: bool

    :return: Check status depending on the requested mode (raise or bool)
    :rtype: bool
    """

    # Convert single type to a list of one type
    if type(allowed_types) != list:
        allowed_types = [allowed_types]

    for t in allowed_types:
        if type(value) is t or (t is None and value is None):
            break
    else:
        if raise_exception:
            vt = type(value)
            raise TypeError("Type of {var_name} shall be one of {allowed_types}, not {vt}".format(**locals()))
        else:
            return False
    return True

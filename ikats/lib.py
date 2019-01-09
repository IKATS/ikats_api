def check_type(value, allowed_types, var_name="variable", raise_exception=True):
    """
    Raises TypeError or returns False if value doesn't belong to the allowed types

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
        if type(value) == t or (t is None and value is None):
            break
    else:
        if raise_exception:
            vt = type(value)
            raise TypeError("Type of {var_name} shall belong to {allowed_types}, not {vt}".format(**locals()))
        else:
            return False
    return True


def check_is_fid_valid(fid, raise_exception=True):
    """
    Check if FID is well formed

    :param fid:
    :type fid: str
    :param raise_exception: Indicate if an exception shall be raised (True, default) or not (False)
    :type raise_exception: bool

    :return: the status of the check
    :rtype: bool

    :raises TypeError: if FID is invalid
    :raises ValueError: if FID is not well formatted
    """
    check_status = True
    if type(fid) != str:
        if raise_exception:
            raise TypeError("Type of '%s' shall be str, not %s" % (fid, type(fid)))
        else:
            check_status = False
    if len(fid) < 3:
        if raise_exception:
            raise ValueError("fid shall have at least 3 characters" % fid)
        else:
            check_status = False
    if " " in fid:
        if raise_exception:
            raise ValueError("fid shall not contains spaces: '%s'" % fid)
        else:
            check_status = False
    return check_status


def check_is_valid_epoch(value, raise_exception=True):
    """
    Check if the value is a valid EPOCH value

    :param value: value to check
    :param raise_exception: Indicate if an exception shall be raised (True, default) or not (False)
    :type raise_exception: bool

    :return: the status of the check
    :rtype: bool

    :raises TypeError: if value is invalid
    :raises ValueError: if value is not well formatted
    """
    check_status = True
    if type(value) != int:
        if raise_exception:
            raise TypeError("Type of '%s' shall be int, not %s" % (value, type(value)))
        else:
            check_status = False
    if value < 0:
        if raise_exception:
            raise ValueError("value shall be positive integer" % value)
        else:
            check_status = False
    return check_status


def check_is_valid_ds_name(value, raise_exception=True):
    """
    Check if the value is a valid Dataset name

    :param value: value to check

    :param raise_exception: Indicate if an exception shall be raised (True, default) or not (False)
    :type raise_exception: bool

    :return: the status of the check
    :rtype: bool

    :raises TypeError: if value is invalid
    :raises ValueError: if value is not well formatted
    """
    check_status = True
    if type(value) != str:
        if raise_exception:
            raise TypeError("Dataset shall be str, not %s", type(value))
        else:
            check_status = False
    if len(value) == 0:
        if raise_exception:
            raise ValueError("Dataset shall have characters", value)
        else:
            check_status = False
    if " " in value:
        if raise_exception:
            raise ValueError("Dataset shall not contains spaces", value)
        else:
            check_status = False
    return check_status



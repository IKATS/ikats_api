# -*- coding: utf-8 -*-
"""
Copyright 2019 CS Systèmes d'Information

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
from enum import Enum


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

    :returns: Check status depending on the requested mode (raise or bool)
    :rtype: bool

    :raises TypeError: if value doesn't belong to the allowed types
    """

    # Convert single type to a list of one type
    if not isinstance(allowed_types, list):
        allowed_types = [allowed_types]

    value_type = type(value)

    if (value is None and None in allowed_types) or (value_type in allowed_types):
        return True
    if raise_exception:
        raise TypeError("Type of %s shall belong to %s, not %s" % (var_name, allowed_types, value_type))
    return False


def check_is_fid_valid(fid, raise_exception=True):
    """
    Check if FID is well formed

    :param fid: functional ID
    :param raise_exception: Indicate if an exception shall be raised (True, default) or not (False)

    :type fid: str
    :type raise_exception: bool

    :returns: the status of the check
    :rtype: bool

    :raises TypeError: if FID is invalid
    :raises ValueError: if FID is not well formatted
    """
    if fid is None:
        if raise_exception:
            raise ValueError("FID shall be set")
        return False
    if not isinstance(fid, str):
        if raise_exception:
            raise TypeError("Type of fid: '%s' shall be str, not %s" % (fid, type(fid)))
        return False
    if len(fid) < 3:
        if raise_exception:
            raise ValueError("fid shall have at least 3 characters: '%s'" % fid)
        return False
    if " " in fid:
        if raise_exception:
            raise ValueError("fid shall not contains spaces: '%s'" % fid)
        return False
    return True


def check_is_valid_epoch(value, raise_exception=True):
    """
    Check if the value is a valid EPOCH value

    :param value: value to check
    :param raise_exception: Indicate if an exception shall be raised (True, default) or not (False)

    :type value: str
    :type raise_exception: bool

    :returns: the status of the check
    :rtype: bool

    :raises TypeError: if value is invalid
    :raises ValueError: if value is not well formatted
    """
    if not isinstance(value, int):
        if raise_exception:
            raise TypeError("Type of '%s' shall be int, not %s" % (value, type(value)))
        return False
    if value < 0:
        if raise_exception:
            raise ValueError("value shall be positive integer! %s" % value)
        return False
    return True


def check_is_valid_ds_name(value, raise_exception=True):
    """
    Check if the value is a valid Dataset name

    :param value: value to check
    :param raise_exception: Indicate if an exception shall be raised (True, default) or not (False)

    :type value: str
    :type raise_exception: bool

    :returns: the status of the check
    :rtype: bool

    :raises TypeError: if value is invalid
    :raises ValueError: if value is not well formatted
    """
    if not isinstance(value, str):
        if raise_exception:
            raise TypeError("Dataset shall be str, not %s" % type(value))
        return False
    if len(value) < 3:
        if raise_exception:
            raise ValueError("Dataset %s shall have characters" % value)
        return False
    if " " in value:
        if raise_exception:
            raise ValueError("Dataset %s shall not contains spaces" % value)
        return False
    return True


class MDType(Enum):
    """
    Enum used for Data types of Metadata
    """
    STRING = "string"
    DATE = "date"
    NUMBER = "number"
    COMPLEX = "complex"

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

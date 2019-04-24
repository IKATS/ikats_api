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

from ikats.client.datamodel_client import DatamodelClient
from ikats.exceptions import (IkatsConflictError, IkatsException,
                              IkatsNotFoundError)
from ikats.lib import check_type
from ikats.manager.generic_mgr_ import IkatsGenericApiEndPoint
from ikats.objects import Table


class IkatsTableMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Table management
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dm_client = DatamodelClient(session=self.api.session)

    def new(self, name=None, data=None):
        """
        Creates an empty table locally

        :param name: (optional) name of the Table
        :param data: (optional) data of the table (as a JSON)

        :type name: str or None
        :type data: dict or None

        :return: the Table object
        :rtype: Table

        :raises IkatsConflictError: if table already exist
        """
        check_type(value=name, allowed_types=[str, None], var_name="name", raise_exception=True)
        check_type(value=data, allowed_types=[dict, None], var_name="data", raise_exception=True)
        try:
            self.dm_client.table_read(name=name)
        except IkatsNotFoundError:
            return Table(api=self.api, name=name, data=data)
        raise IkatsConflictError("Table already exist. Try using `get()` method")

    def get(self, name):
        """
        Reads the data blob content: for the unique table identified by id.

        :param name: the id key of the raw table to get data from
        :type name: str

        :returns: the content data stored.
        :rtype: bytes or str or object

        :raises IkatsNotFoundError: no resource identified by ID
        :raises IkatsException: any other error
        """
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        data = self.dm_client.table_read(name=name)
        return Table(api=self.api, name=name, data=data)

    def save(self, data, name=None, raise_exception=True):
        """
        Create a table

        If name or description is provided,
        the method will overwrite the corresponding fields inside the data.

        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param data: data to store
        :param name: name of the table (optional)
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type data: dict
        :type name: str or None
        :type raise_exception: bool

        :returns: the status of deletion (True=deleted, False otherwise)
        :rtype: bool

        :raises IkatsConflictError: if Table *name* already exists in database

        """
        check_type(value=data, allowed_types=dict, var_name="data", raise_exception=True)
        check_type(value=name, allowed_types=[str, None], var_name="name", raise_exception=True)
        check_type(value=raise_exception, allowed_types=bool, var_name="raise_exception", raise_exception=True)

        if name is not None:
            data['table_desc']['name'] = name
        try:
            self.dm_client.table_create(data=data)
        except IkatsException:
            if raise_exception:
                raise
            return False
        return True

    def delete(self, name, raise_exception=True):
        """
        Delete a table
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param name: the name of the table to delete
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type name: str
        :type raise_exception: bool

        :returns: the status of deletion (True=deleted, False otherwise)
        :rtype: bool

        :raises IkatsNotFoundError: if table not found in database
        """
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)
        check_type(value=raise_exception, allowed_types=bool, var_name="raise_exception", raise_exception=True)

        try:
            self.dm_client.table_delete(name=name)
        except IkatsException:
            if raise_exception:
                raise
            return False
        return True

    def list(self, name=None, strict=True):
        """
        List all tables
        If name is specified, filter by name
        name can contains "*", this character is considered as "any chars" (equivalent to regexp /.*/)

        :param name: name to find
        :param strict: consider name without any wildcards

        :type name: str or None
        :type strict: bool

        :returns: the list of tables matching the requirements
        :rtype: list
        """
        check_type(value=name, allowed_types=[str, None], var_name="name", raise_exception=True)
        check_type(value=strict, allowed_types=bool, var_name="strict", raise_exception=True)

        return self.dm_client.table_list(name=name, strict=strict)

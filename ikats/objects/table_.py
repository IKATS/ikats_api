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
from schema import And, Optional, Schema, SchemaError, Use

from ikats.lib import check_type
from ikats.objects.generic_ import IkatsObject

TABLE_SCHEMA = Schema({
    Optional('table_desc'): {
        'name': And(Use(str)),
        'title': And(Use(str)),
        'desc': And(Use(str)),
    },
    Optional('headers'): {
        Optional('col'): {
            'data': And(Use(list)),
            Optional('default_links'): {'type': And(Use(str)), 'context': And(Use(str))},
            Optional('links'): And(Use(list))
        },
        Optional('row'): {
            'data': And(Use(list)),
            Optional('default_links'): {'type': And(Use(str)), 'context': And(Use(str))},
            Optional('links'): And(Use(list))
        },
    },
    'content': {
        'cells': And(Use(list)),
        Optional('default_links'): {'type': And(Use(str)), 'context': And(Use(str))},
        Optional('links'): And(Use(list))
    }
})


class Table(IkatsObject):
    """
    Table class
    """
    # TODO: Doc
    def __init__(self, api, name=None, data=None):
        """
        See props for members description

        :param api: see IkatsObject
        :param name: Name of the Table
        :param data: Data composing the Table (as list of list)

        :type api:
        :type name: str or None
        :type data: dict
        """

        # Internal variables initialization
        super().__init__(api)
        self.__name = None
        self.__desc = None
        self.__data = dict()

        # Initialization with provided parameters
        self.name = name
        self.data = data

    # TODO: Doc
    @property
    def name(self):
        """
        Name of the dataset
        :rtype:
        """
        return self.__name

    @name.setter
    def name(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="name", raise_exception=True)
        if value is not None:
            self.__name = value
        if self.__data is not None:
            try:
                self.__data['table_desc']['name'] = value
            except KeyError:
                pass

    @property
    def data(self):
        """
        Table data content
        The mapping is close to the JSON format that is exchanged with backend
        :rtype: dict
        """
        return self.__data

    @data.setter
    def data(self, value):
        check_type(value=value, allowed_types=[dict, None], var_name="name", raise_exception=True)
        if value is not None:
            self.is_json_valid(data=value)
            try:
                self.name = value['table_desc']['name']
            except KeyError:
                # The data doesn't contain the name, just skip this part
                pass

        self.__data = value

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Table %s" % self.name

    @staticmethod
    def is_json_valid(data, raise_exception=True):
        """
        Check if the provided JSON (as a dict) contains all necessary information to be considered valid

        :param data: the JSON as dict
        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)

        :type data: dict
        :type raise_exception: bool

        :return: the check status
        :rtype: bool

        :raises SchemaError: if JSON is invalid
        """
        check_type(value=data, allowed_types=dict, var_name="data", raise_exception=True)

        try:
            TABLE_SCHEMA.validate(data=data)
            return True
        except SchemaError:
            if raise_exception:
                raise
            return False

    def save(self, raise_exception=True):
        """
        Save the table to database (creation only, no update available)
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises IkatsConflictError: if Table name already exists in database
        """
        check_type(value=raise_exception, allowed_types=bool, var_name="raise_exception", raise_exception=True)
        return self.api.table.save(data=self.data, name=self.name, raise_exception=raise_exception)

    def delete(self, raise_exception=True):
        """
        Remove the table from database but keep the local object
        Returns a boolean status of the action (True means "OK", False means "errors occurred")

        :param raise_exception: Indicates if exceptions shall be raised (True, default) or not (False)
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises IkatsNotFoundError: if table not found in database
        """
        check_type(value=raise_exception, allowed_types=bool, var_name="raise_exception", raise_exception=True)
        return self.api.table.delete(name=self.name, raise_exception=raise_exception)

    def extract(self, obs_id, items):
        """
        Extract information from a table and format the output as a dict of dict.
        The first key will be the obs_id values taken from the table_content.
        The sub keys will be the items.

        :param obs_id: Column name used as primary key
        :param items: list of other columns to extract

        :type obs_id: str
        :type items: list

        :returns: a dict of dict where first key is the obs_id and the sub keys are the items
        :rtype: dict
        """
        check_type(value=obs_id, allowed_types=str, var_name="obs_id", raise_exception=True)
        check_type(value=items, allowed_types=list, var_name="items", raise_exception=True)

        # 2D array containing the equivalent of the rendered JSON structure
        data_array = []

        try:
            # Get the columns name with a mapping dict
            columns_name = {k: v for v, k in enumerate(self.data["headers"]["col"]["data"])}
        except KeyError:
            raise ValueError("Table content shall contain col headers to know the name of columns")

        try:
            # Fill the 2D array with the content of the header column
            # Skip the first cell by starting at index 1
            data_array = [[x] for x in self.data["headers"]["row"]["data"][1:]]
        except KeyError:
            # No header column present, skip it
            pass

        # Building final computed results
        results = {}
        for line_index, line in enumerate(self.data["content"]["cells"]):
            if len(data_array) < line_index:
                # Fill in the data_array line with an empty list in case there was no header column
                data_array.append([])
            # Extend the current column with the other columns
            data_array[line_index].extend(line)

            first_key_value = data_array[line_index][columns_name[obs_id]]

            if first_key_value in results:
                raise ValueError("Key %s is not unique" % obs_id)

            results[first_key_value] = {}
            for item in items:
                results[first_key_value][item] = data_array[line_index][columns_name[item]]
        return results

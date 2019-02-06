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

from unittest import TestCase

from schema import SchemaError

from ikats import IkatsAPI
from ikats.exceptions import IkatsConflictError, IkatsNotFoundError


class TestTable(TestCase):
    """
    Test Table object
    """
    def test_nominal(self):
        """
        Creation of a Table instance
        """
        api = IkatsAPI()
        table = api.table.new()
        name = "my_table"

        data = {
            "table_desc": {
                "name": name,
                "desc": "table description",
                "title": "Table Title"
            },
            "content": {
                "cells": [[1, 2, 3], [4, 5, 6]]
            }
        }
        table.data = data

        # Standard checks
        self.assertEqual(name, table.name)
        self.assertEqual(data, table.data)
        self.assertEqual(name, str(table))
        self.assertEqual("Table %s" % name, repr(table))

        # Save and delete (nominal)
        self.assertTrue(api.table.save(data=data))
        self.assertTrue(api.table.delete(name=name, raise_exception=False))

        # Change table name
        other_name = name + '2'
        table.name = other_name
        self.assertEqual(other_name, table.name)

        # Save and delete table with another name
        self.assertTrue(api.table.save(name=other_name, data=table.data))
        self.assertTrue(api.table.delete(name=other_name, raise_exception=False))

        # Save table again (after deleting it)
        self.assertTrue(table.save(raise_exception=False))

        # Saving already saved is forbidden. Checking 4 ways to get the KO status
        self.assertFalse(table.save(raise_exception=False))
        self.assertFalse(api.table.save(data=table.data, raise_exception=False))
        with self.assertRaises(IkatsConflictError):
            table.save(raise_exception=True)
        with self.assertRaises(IkatsConflictError):
            api.table.save(data=data, raise_exception=True)

        # Clean
        self.assertTrue(table.delete(raise_exception=False))
        self.assertFalse(table.delete(raise_exception=False))

    def test_bad_json(self):
        """
        Check JSON checker
        """
        api = IkatsAPI()
        table = api.table.new()
        name = "my_table"

        data = {
            "unknown field": {
                "name": name,
                "desc": "table description",
                "title": "Table Title"
            },
            "content": {
                "cells": [[1, 2, 3], [4, 5, 6]]
            }
        }
        self.assertFalse(table.is_json_valid(data, raise_exception=False))
        with self.assertRaises(SchemaError):
            table.is_json_valid(data)

    def test_exception(self):
        """
        Tests exception that can be raised
        """
        api = IkatsAPI()
        name = "my_table"

        self.assertEqual(0, len(api.table.list()))

        # cleanup
        api.table.delete(name=name, raise_exception=False)

        data = {
            "table_desc": {
                "name": name,
                "desc": "table description",
                "title": "Table Title"
            },
            "content": {
                "cells": [[1, 2, 3], [4, 5, 6]]
            }
        }

        table = api.table.new(name=name, data=data)
        table.save()

        table_2 = api.table.get(name=name)
        self.assertEqual(name, table_2.data["table_desc"]["name"])

        with self.assertRaises(IkatsConflictError):
            api.table.new(name=name, data=data)
        table.delete()

        with self.assertRaises(IkatsNotFoundError):
            api.table.get(name="unknown_table")

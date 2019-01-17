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

from ikats.api import IkatsAPI
from ikats.lib import MDType
from ikats.exceptions import IkatsNotFoundError
from ikats.tests.lib import delete_ts_if_exists


class TestMetadata(TestCase):

    def test_types(self):
        """
        Creation of a Metadata instance
        """

        # Init
        api = IkatsAPI()
        delete_ts_if_exists("MyTS")
        ts = api.ts.new(fid="MyTS")

        # Provide a number, get a string
        ts.metadata.set(name="myMD", value=42, dtype=MDType.STRING)
        self.assertEqual("42", ts.metadata.get(name="myMD"))
        self.assertEqual(str, type(ts.metadata.get(name="myMD")))
        self.assertEqual(MDType.STRING, ts.metadata.get_type(name="myMD"))

        # Provide a number, get a string (default)
        ts.metadata.set(name="myMD", value=42)
        self.assertEqual("42", ts.metadata.get(name="myMD"))
        self.assertEqual(str, type(ts.metadata.get(name="myMD")))
        self.assertEqual(MDType.STRING, ts.metadata.get_type(name="myMD"))

        # Provide an int as string, get an int
        ts.metadata.set(name="myMD", value="42", dtype=MDType.NUMBER)
        self.assertEqual(int, type(ts.metadata.get(name="myMD")))
        self.assertEqual(MDType.NUMBER, ts.metadata.get_type(name="myMD"))

        # Provide an float as string, get a float
        ts.metadata.set(name="myMD", value="42.5", dtype=MDType.NUMBER)
        self.assertEqual(float, type(ts.metadata.get(name="myMD")))
        self.assertEqual(MDType.NUMBER, ts.metadata.get_type(name="myMD"))

        # Provide a date as string, get an int
        ts.metadata.set(name="myMD", value="42", dtype=MDType.DATE)
        self.assertEqual(42, ts.metadata.get(name="myMD"))
        self.assertEqual(MDType.DATE, ts.metadata.get_type(name="myMD"))

        # Provide a date as int, get an int
        ts.metadata.set(name="myMD", value=1564856, dtype=MDType.DATE)
        self.assertEqual(1564856, ts.metadata.get(name="myMD"))
        self.assertEqual(MDType.DATE, ts.metadata.get_type(name="myMD"))

        ts.delete()

    def test_nominal(self):
        # Init
        api = IkatsAPI()
        delete_ts_if_exists(fid="MyTS")
        ts_1 = api.ts.new(fid="MyTS")

        # Create new MD
        ts_1.metadata.set(name="myMD", value=42, dtype=MDType.STRING)

        # Save it
        self.assertTrue(ts_1.metadata.save())

        # In a new object, get it
        ts_2 = api.ts.get(fid="MyTS")
        self.assertEqual("42", ts_2.metadata.get("myMD"))

        # Mark deleted in first object
        ts_1.metadata.delete(name="myMD")

        # MD not present in first object
        with self.assertRaises(IkatsNotFoundError):
            ts_1.metadata.get("myMD")

        # MD still present in second object (first not synced with database)
        self.assertEqual("42", ts_2.metadata.get("myMD"))

        # Save it
        self.assertTrue(ts_1.metadata.save())

        # MD still present in second object (first not synced with database)
        self.assertEqual("42", ts_2.metadata.get("myMD"))

        # Get last updates
        ts_2.metadata.fetch()

        # MD not present in second object
        with self.assertRaises(IkatsNotFoundError):
            ts_2.metadata.get("myMD")

        # MD not present in just created object
        ts_3 = api.ts.get(fid="MyTS")
        with self.assertRaises(IkatsNotFoundError):
            ts_3.metadata.get("myMD")
        with self.assertRaises(IkatsNotFoundError):
            ts_3.metadata.get_type("myMD")

        md = ts_2.metadata
        self.assertEqual("%s Metadata associated to TSUID %s" % (len(md.data), ts_2.tsuid), repr(md))
        self.assertEqual(len(md.data), len(md))

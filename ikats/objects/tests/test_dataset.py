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

from ikats import IkatsAPI
from ikats.exceptions import IkatsConflictError, IkatsInputError, IkatsNotFoundError
from ikats.tests.lib import delete_ts_if_exists


class TestDataset(TestCase):
    def test_new(self):
        """
        Creation of a Dataset instance
        """
        api = IkatsAPI()

        # Empty
        ds = api.ds.new()
        self.assertEqual(None, ds.name)
        self.assertEqual("", ds.desc)
        self.assertEqual(0, len(ds))
        self.assertEqual(len(ds.ts), len(ds))

        # Minimal information
        name = "my_new_dataset"
        description = "My_new_description"
        ds.name = name
        self.assertEqual(name, ds.name)
        ds.desc = description
        self.assertEqual(description, ds.desc)

        with self.assertRaises(IkatsConflictError):
            api.ds.new(name="Portfolio")

        self.assertEqual(ds.name, str(ds))
        self.assertEqual("Dataset %s" % ds.name, repr(ds))

    def test_get(self):
        api = IkatsAPI()

        # Empty
        ds = api.ds.get(name="Portfolio")
        self.assertEqual("Portfolio", ds.name)
        self.assertEqual(13, len(ds.ts))

    def test_add(self):
        api = IkatsAPI()
        ts_list1 = [api.ts.new() for _ in range(10)]
        ts_list2 = [api.ts.new() for _ in range(11, 20)]

        # Direct add
        ds1 = api.ds.new(ts=ts_list1)

        # Add list of TS
        ds2 = api.ds.new()
        ds2.add_ts(ts_list2)

        # Combine
        ds3 = ds1 + ds2
        self.assertEqual(len(ts_list1) + len(ts_list2), len(ds3))
        self.assertEqual(len(ds3.ts), len(ds3))
        ts_list3 = ts_list1 + ts_list2
        self.assertEqual(ts_list3, ds3.ts)

        # add single TS
        ds3.add_ts("42")
        self.assertEqual(len(ts_list1) + len(ts_list2) + 1, len(ds3))
        self.assertEqual("42", ds3.ts[-1].tsuid)

    def test_bad_set(self):
        api = IkatsAPI()
        ds = api.ds.new()

        for value in [42, [1, 2, 3], {'k': 'v'}]:
            with self.assertRaises(TypeError):
                ds.name = value
            with self.assertRaises(TypeError):
                ds.desc = value

    def test_create_delete(self):

        # Cleanup
        api = IkatsAPI()
        for x in range(10):
            delete_ts_if_exists(fid="FID_TEST_%s" % x)

        # Setup
        ts_list1 = [api.ts.new(fid="FID_TEST_%s" % x) for x in range(10)]
        ds1 = api.ds.new(name="DS_TEST", desc="my description", ts=ts_list1)

        # Test deletion with unknown dataset
        with self.assertRaises(IkatsNotFoundError):
            api.ds.delete(name=ds1)
        self.assertFalse(ds1.delete(raise_exception=False))

        # Check
        if not ds1.save(raise_exception=False):
            self.fail("Dataset should have been saved")

        # Test deletion
        self.assertTrue(ds1.delete(deep=True))

        # Test empty TS list
        ds1.ts = []
        with self.assertRaises(ValueError):
            ds1.save()

        # No TSUID assigned to ts_list2 should produce an IkatsInputError when saving
        ts_list2 = [api.ts.new() for _ in range(10)]
        ds2 = api.ds.new(name="DS_TEST2", desc="my description", ts=ts_list2)
        with self.assertRaises(IkatsInputError):
            ds2.save()

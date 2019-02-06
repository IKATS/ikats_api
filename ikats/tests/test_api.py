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
from ikats.exceptions import IkatsNotFoundError, IkatsServerError
from ikats.extra.timeseries import gen_random_ts
from ikats.tests.lib import delete_ts_if_exists


class TestApi(TestCase):
    """
    Main API tests
    """
    def test_ds(self):
        """
        Tests main operations on Datasets
        """
        # DS list
        api = IkatsAPI(host="http://localhost", port=80, emulate=False)
        ds_list = api.ds.list()
        self.assertLess(0, len(ds_list))

    def test_ts(self):
        """
        Tests main operations on Timeseries
        """
        # TS list
        api = IkatsAPI(host="http://localhost", port=80, emulate=False)
        ts_list = api.ts.list()
        self.assertLess(0, len(ts_list))

        with self.assertRaises(ValueError):
            api.ts.get(fid="fid_set", tsuid="tsuid_set")

        # TSUID<->FID converters
        my_fid = "TEST_FID"
        delete_ts_if_exists(fid=my_fid)
        ts = api.ts.new(fid=my_fid, data=gen_random_ts(sd=1000000000000, nb_points=2, period=1000))
        ts.save()
        my_tsuid = api.ts.fid2tsuid(fid=my_fid)
        self.assertEqual(my_tsuid, ts.tsuid)
        new_fid = api.ts.tsuid2fid(tsuid=ts.tsuid)
        self.assertEqual(my_fid, new_fid)

    def test_op(self):
        """
        Tests main operations on Operators
        """
        # OP list
        api = IkatsAPI(host="http://localhost", port=80, emulate=False)
        op_list = api.op.list()
        self.assertLess(0, len(op_list))

        op = op_list[0]
        op.fetch()

        # OP.results
        with self.assertRaises(IkatsNotFoundError):
            api.op.results(pid="unknown")

    def test_table(self):
        """
        Tests main operations on Tables
        """
        api = IkatsAPI(host="http://localhost", port=80, emulate=False)

        tables_list = api.table.list()
        self.assertEqual(0, len(tables_list))

        # see bugs #2935
        # with self.assertRaises(IkatsNotFoundError):
        with self.assertRaises(IkatsServerError):
            api.table.delete(name="unknownTable")

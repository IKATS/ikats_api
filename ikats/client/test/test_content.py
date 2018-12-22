"""
Copyright 2018 CS Systèmes d'Information

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

from numpy import arange as np_arange
from numpy import array as np_array
from numpy import array_equal as np_equals
from numpy import random

from ikats.client.test.ts_content import TsManagement, TsBuilder, step_pattern


class TestTsManagement(TestCase):
    """
    Tests TsManagement module
    """

    @classmethod
    def setUpClass(cls):
        super(TestTsManagement, cls).setUpClass()

        # ts_one=
        # [[ 0  1]
        #  [ 2  3]
        #  [ 4  5]
        #   ...
        #  [92 93]
        #  [94 95]
        #  [96 97]
        #  [98 99]]
        cls.ts_one = np_arange(100).reshape(50, 2)

        # ts_two=
        # [[198 199]
        #  [196 197]
        #  [194 195]
        #  [192 193]
        #    ...
        #  [104 105]
        #  [102 103]
        #  [100 101]]
        #
        cls.ts_two = cls.ts_one[50::-1] + 100

        # reverse sort of ts_two:
        #
        # ts_inverse=
        # [[100 101]
        #  [102 103]
        #  [104 105]
        #    ...
        #  [194 195]
        #  [196 197]
        #  [198 199]]
        #
        cls.ts_inverse = cls.ts_two[::-1]

    def test_sort(self):
        ts_two_sorted = TsManagement.sort(self.ts_two)
        self.assertTrue(np_equals(self.ts_inverse, ts_two_sorted))

    def test_merge(self):
        ts_merge = TsManagement.merge(self.ts_one, self.ts_two)
        ts_merge_bis = TsManagement.merge(self.ts_one, self.ts_inverse)
        self.assertTrue(np_equals(ts_merge, ts_merge_bis), "Test1 Merge failed")

    def test_merge_2(self):
        ts_merge_duppl_test2_one = TsManagement.merge(self.ts_one, self.ts_one[0:10])
        self.assertTrue(np_equals(ts_merge_duppl_test2_one, self.ts_one),
                        "Test2 Merge failed: ignored dooblons")

    def test_merge_3(self):
        ts_merge_duppl_test3 = TsManagement.merge(self.ts_one, self.ts_one[10:0:-1])
        self.assertTrue(np_equals(ts_merge_duppl_test3, self.ts_one),
                        "Test3 Merge failed: ignored dooblons")

    def test_merge_4(self):
        ts_merge_test4 = TsManagement.merge(self.ts_one, np_array([[98, 99], [200, 201]]))
        self.assertTrue((len(ts_merge_test4) - len(self.ts_one) == 1)
                        or (ts_merge_test4[-1][0] == 200) or (ts_merge_test4[-1][1] == 201),
                        "Test4 Merge failed: some added, some others not added")

    def test_select_period(self):
        selected_p1 = TsManagement.select_period(self.ts_one, sd=None, ed=None)
        self.assertTrue(np_equals(self.ts_one, selected_p1),
                        "Test1 Select period failed: no constraints")

    def test_select_period_2(self):
        selected_p2 = TsManagement.select_period(self.ts_one, None, 18)
        selected_p2_bis = TsManagement.select_period(self.ts_one, None, 19)
        selected_p2_ref = self.ts_one[0:10]
        self.assertTrue(np_equals(selected_p2, selected_p2_bis)
                        or not np_equals(selected_p2, selected_p2_ref),
                        "Test2 Select period failed: ed constraint only")

    def test_select_period_3(self):
        selected_p3 = TsManagement.select_period(self.ts_one, 90, None)
        selected_p3_bis = TsManagement.select_period(self.ts_one, 89, None)
        selected_p3_ref = self.ts_one[-5:]
        self.assertTrue(np_equals(selected_p3_bis, selected_p3_ref)
                        or not np_equals(selected_p3, selected_p3_ref),
                        "Test3 Select period failed: sd constraint only")

    def test_select_period_4(self):
        selected_p4 = TsManagement.select_period(self.ts_one, 90, 91)
        selected_p4_bis = TsManagement.select_period(self.ts_one, 89, 90)
        selected_p4_ref = self.ts_one[-5:-4]
        self.assertTrue(np_equals(selected_p4_bis, selected_p4_ref)
                        or not np_equals(selected_p4, selected_p4_ref),
                        "Test4 Select period failed: constraints sd+ed => single point")

    def test_select_period_5(self):
        selected_p5 = TsManagement.select_period(self.ts_one, 98, 180)
        selected_p5_bis = TsManagement.select_period(self.ts_one, 98, 98)
        selected_p5_ref = self.ts_one[-1:]
        self.assertTrue(np_equals(selected_p5_bis, selected_p5_ref)
                        or not np_equals(selected_p5, selected_p5_ref),
                        "Test5 Select period failed: constraints sd+ed => last single point")

    def test_select_period_6(self):
        selected_p6 = TsManagement.select_period(self.ts_one, 6, 12)
        selected_p6_bis = TsManagement.select_period(self.ts_one, 5, 13)
        selected_p6_ref = self.ts_one[3:7]
        self.assertTrue(np_equals(selected_p6_bis, selected_p6_ref)
                        or not np_equals(selected_p6, selected_p6_ref),
                        "Test6 Select period failed: constraints sd+ed => intermediate points")

    def test_select_period_7(self):
        selected_p7 = TsManagement.select_period(self.ts_one, -10, 6)
        selected_p7_bis = TsManagement.select_period(self.ts_one, 0, 7)
        selected_p7_ref = self.ts_one[0:4]
        self.assertTrue(np_equals(selected_p7_bis, selected_p7_ref)
                        or not np_equals(selected_p7, selected_p7_ref),
                        "Test7 Select period failed: constraints sd+ed => first points")


class TestTsBuilder(TestCase):

    def test_add_points(self):
        my_builder = TsBuilder()
        sd = 1000000
        my_builder.add_points(sd, sd + 50, 1, lambda x: (x - sd) * 2.0)
        ts = my_builder.get_ts()
        self.assertTrue(len(ts) == 51)

        self.assertTrue(ts[0][0] == sd)
        self.assertTrue(ts[-1][0] == sd + 50)

        self.assertTrue(ts[0][1] == 0.0)
        self.assertEqual(ts[-1][1], 100.0)

    def test_add_pattern_points(self):
        sd = 1000000
        my_builder = TsBuilder().add_points(sd, sd + 50, 1, lambda x: float(random.normal(0.0, 1.0)))

        # "add" same pattern at different intervals, scales
        # => replace computed points from the resized and translated pattern
        #

        my_builder.add_pattern_points(1000033, 1000043, 1, step_pattern, translate_value=0.0, scale_value=1.0)
        my_builder.add_pattern_points(1000005, 1000010, 1, step_pattern, translate_value=10.0)
        my_builder.add_pattern_points(1000013, 1000018, 1, step_pattern, translate_value=0.0, scale_value=5.0)
        ts = my_builder.get_ts()

        self.assertEqual(len(ts), 51)

        self.assertEqual(ts[5][1], 9.0)
        self.assertEqual(ts[10][1], 11.0)

        self.assertEqual(ts[13][1], -5.0)
        self.assertEqual(ts[18][1], 5.0)

        self.assertEqual(ts[33][1], -1)
        self.assertEqual(ts[43][1], 1)

from unittest import TestCase

from ikats import Timeseries
from ikats.dataset_ import Dataset


class TestTimseries(TestCase):
    def test_new(self):
        """
        Creation of a Timseries instance
        """
        # Empty
        ts = Timeseries()
        self.assertEqual(None, ts.tsuid)
        self.assertEqual("", ts.fid)
        self.assertEqual(0, len(ts))

        # Minimal information
        name = "my_new_dataset"
        description = "My_new_description"
        ts.name = name
        self.assertEqual(name, ts.name)
        ts.description = description
        self.assertEqual(description, ts.description)

    def test_add(self):
        ts_list1 = [Timeseries(tsuid=str(x)) for x in range(10)]
        ts_list2 = [Timeseries(tsuid=str(x)) for x in range(11, 20)]

        # Direct add
        ds1 = Dataset(ts=ts_list1)

        # Add using method
        ds2 = Dataset()
        ds2.add_ts(ts_list2)

        # Combine
        ds3 = ds1 + ds2
        self.assertEqual(len(ts_list1) + len(ts_list2), len(ds3))
        self.assertEqual(len(ds3.ts), len(ds3))

        # Combine
        ds3.ts.append("42")
        self.assertEqual(len(ts_list1) + len(ts_list2) + 1, len(ds3))

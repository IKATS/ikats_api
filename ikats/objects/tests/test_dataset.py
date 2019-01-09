from unittest import TestCase

from ikats import IkatsAPI


class TestDataset(TestCase):
    def test_new(self):
        """
        Creation of a Dataset instance
        """
        api = IkatsAPI()

        # Empty
        ds = api.dataset()
        self.assertEqual(None, ds.name)
        self.assertEqual("", ds.description)
        self.assertEqual(0, len(ds))
        self.assertEqual(len(ds.ts), len(ds))

        # Minimal information
        name = "my_new_dataset"
        description = "My_new_description"
        ds.name = name
        self.assertEqual(name, ds.name)
        ds.description = description
        self.assertEqual(description, ds.description)

    def test_add(self):
        api = IkatsAPI()
        ts_list1 = [api.timeseries(tsuid=str(x)) for x in range(10)]
        ts_list2 = [api.timeseries(tsuid=str(x)) for x in range(11, 20)]

        # Direct add
        ds1 = api.dataset(ts=ts_list1)

        # Add list of TS
        ds2 = api.dataset()
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

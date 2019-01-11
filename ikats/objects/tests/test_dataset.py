from unittest import TestCase

from ikats import IkatsAPI
from ikats.exceptions import IkatsConflictError


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

    def test_get(self):
        api = IkatsAPI()

        # Empty
        ds = api.ds.get(name="Portfolio")
        self.assertEqual("Portfolio", ds.name)
        self.assertEqual(13, len(ds.ts))

    def test_add(self):
        api = IkatsAPI()
        ts_list1 = [api.ts.new() for x in range(10)]
        ts_list2 = [api.ts.new() for x in range(11, 20)]

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

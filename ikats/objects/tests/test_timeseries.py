import numpy as np
from unittest import TestCase

from ikats.api import IkatsAPI
from ikats.extra.timeseries import gen_random_ts
from ikats.objects.tests.lib import delete_ts_if_exists


class TestTimeseries(TestCase):
    ts_to_delete = []

    def test_new_local(self):
        """
        Creation of a Timeseries instance
        """
        api = IkatsAPI()

        # Empty TS
        ts = api.ts.new()

        # Check
        self.assertIsNone(ts.tsuid)
        self.assertIsNone(ts.fid)
        self.assertEqual(0, len(ts.data))
        self.assertEqual(0, len(ts))

    def test_from_scratch(self):
        api = IkatsAPI()

        # Create a new TS
        ts = api.ts.new()
        ts.data = gen_random_ts(sd=1000000000000, ed=1000000010000, nb_points=10)

        # Check
        self.assertEqual(10, len(ts))
        # No FID provided
        with self.assertRaises(ValueError):
            ts.save()

        ts.fid = "TEST_TS"
        ts.save()

        # TSUID has been set by the save action
        self.assertIsNotNone(ts.tsuid)

        # Minimum Metadata has been computed
        self.assertEqual(ts.data[0][0], ts.metadata.get("ikats_start_date"))
        self.assertEqual(ts.data[-1][0], ts.metadata.get("ikats_end_date"))
        self.assertEqual(len(ts.data), ts.metadata.get("qual_nb_points"))

        # Delete the TS
        ts.delete()

        # see bugs #2738
        # with self.assertRaises(IkatsNotFoundError):
        #    ts.delete()
        # self.assertFalse(ts.delete(raise_exception=False))

    def test_new_with_creation(self):
        """
        Creation of a Timeseries instance with reservation of the FID
        """
        fid = "TEST_TS"

        api = IkatsAPI()
        delete_ts_if_exists(fid=fid)

        ts = api.ts.new(fid=fid)

        # TSUID is filled
        self.assertIsNotNone(ts.tsuid)
        self.assertEqual(fid, ts.fid)

        # No points are present
        self.assertEqual(0, len(ts.data))
        self.assertEqual(0, len(ts))

        # Add points
        ts.data = gen_random_ts(sd=1000000000000, ed=1000000010000, period=1000)
        self.assertEqual(10, len(ts.data))

        # Save TS (using the api method)
        api.ts.save(ts)

        # create a new instance of the same TS
        ts2 = api.ts.get(fid=fid)

        # Compare written data with read data
        self.assertEqual(len(ts.data), len(ts2.data))
        self.assertTrue(np.allclose(
            np.array(ts.data, dtype=np.float64),
            np.array(ts2.data, dtype=np.float64),
            atol=1e-2))

        # Delete TS using no exception raise
        self.assertTrue(api.ts.delete(ts=ts2, raise_exception=False))

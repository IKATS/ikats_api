from unittest import TestCase

from ikats.api import IkatsAPI


class TestTimeseries(TestCase):

    def test_new(self):
        """
        Creation of a Timeseries instance
        """
        # Empty
        api = IkatsAPI()

        # Cleanup
        api.ts.delete(api.timeseries(fid="MyTS"), raise_exception=False)

        ts = api.timeseries()

        self.assertEqual(None, ts.tsuid)
        self.assertEqual(None, ts.fid)
        self.assertEqual(0, len(ts.data))
        self.assertEqual(0, len(ts))

        # Minimal data
        ts = api.ts.create_ref(fid="MyTS")
        self.assertEqual("MyTS", ts.fid)
        self.assertEqual(None, ts.tsuid)

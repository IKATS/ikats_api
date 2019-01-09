from unittest import TestCase

from ikats import IkatsAPI


class TestApi(TestCase):
    def test_ds_list(self):
        # Default session
        api = IkatsAPI(host="http://localhost", port=80)
        ds_list = api.ds.list()
        self.assertLess(0, len(ds_list))

    def test_ts_list(self):
        api = IkatsAPI(host="http://localhost", port=80)
        ts_list = api.ts.list()
        self.assertLess(0, len(ts_list))

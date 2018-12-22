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

import logging
import re
import unittest
from unittest import TestCase

# Documentation about 'httpretty' module: https://github.com/gabrielfalcao/httpretty
import httpretty
import mock
import numpy as np

from ikats.client import TemporalDataMgr
from ikats.client.temporal_data_mgr import DTYPE

# Flag to set to True to use the real servers (setting it to False will use a fake local server)
USE_REAL_SERVER = False


# Address of the real server to use for tests
TEST_HOST = ""
TEST_PORT = int(CF.get('cluster', 'client.port'))

TEST_OTSDB_HOST = CF.get('cluster', 'opentsdb.read.ip')
TEST_OTSDB_PORT = int(CF.get('cluster', 'opentsdb.read.port'))
ROOT_URL = 'http://%s:%s/TemporalDataManagerWebApp/webapi' % (TEST_HOST, TEST_PORT)
DIRECT_ROOT_URL = 'http://%s:%s/api' % (TEST_OTSDB_HOST, TEST_OTSDB_PORT)

# Disable real connection depending on the usage of real or fake server
httpretty.HTTPretty.allow_net_connect = USE_REAL_SERVER

LOGGER = logging.getLogger('ikats.core.resource.client.rest_client')

# Defines the log level to DEBUG
LOGGER.setLevel(logging.DEBUG)

# Log format
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')

# Create handler that will redirect log entries to STDOUT
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setLevel(logging.DEBUG)
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)

META_DATA_LIST = {}


# noinspection PyUnusedLocal
def import_md_mock(self, tsuid, name, value, data_type=DTYPE.string, force_update=False):
    """
    Mock of the import of meta data function to verify the imported meta data
    :param self:
    :param tsuid:
    :param name:
    :param value:
    :param data_type:
    :param force_update:
    :return:
    """
    if tsuid not in META_DATA_LIST:
        META_DATA_LIST[tsuid] = {}

    if name not in META_DATA_LIST[tsuid] or force_update:
        META_DATA_LIST[tsuid][name] = {'value': value, 'dtype': data_type}
        return True
    return False


# noinspection PyUnusedLocal
def get_md_mock(self, ts_list):
    """
    Mock of the get of meta data function to verify the imported meta data
    :param self:
    :param ts_list:
    :return:
    """
    results = {}
    for tsuid in ts_list:
        if tsuid in META_DATA_LIST:
            results[tsuid] = {}
            for md in META_DATA_LIST[tsuid]:
                try:
                    results[tsuid][md] = META_DATA_LIST[tsuid][md]['value']
                except Exception:
                    pass
    return results


def fake_server(func):
    """
    Decorator used to activate (or not) the fake server
    It depends on ``use_real_server`` boolean global variable

    :param func: decorated function
    :return: the function decorated or not with fake server activation
    """
    if not USE_REAL_SERVER:
        return httpretty.activate(func)
    else:
        return func


# noinspection PyTypeChecker
class TestTemporalDataMgr(TestCase):
    """
    Test of the TemporalDataMgr class
    """

    @fake_server
    def test_import_fid(self):
        """
        Tests the import of a functional identifier
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/funcId/test_TSUID/test_FID' % ROOT_URL,
            body='OK',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        # Implicit data type
        tdm.import_fid(
            tsuid='test_TSUID',
            fid='test_FID')
        # If something wrong happens, an assert will be raised by import_fid

    @fake_server
    def test_delete_fid(self):
        """
        Tests the deletion of a functional identifier
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.DELETE,
            '%s/metadata/funcId/test_TSUID' % ROOT_URL,
            body='OK',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        # Implicit data type
        tdm.delete_fid(tsuid='test_TSUID')
        # If something wrong happens, an assert will be raised by delete_fid

    @fake_server
    def test_get_fid(self):
        """
        Tests the deletion of a functional identifier
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/funcId/test_TSUID' % ROOT_URL,
            body="""
                {
                    "tsuid":"test_TSUID",
                    "funcId":"MyFID"
                }""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        # Implicit data type
        fid = tdm.get_fid(tsuid='test_TSUID')
        self.assertEqual(fid, "MyFID")

    @fake_server
    def test_import_meta_data(self):
        """
        Tests the import of meta data (nominal)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='OK',
            status=200
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_md_typed/value_of_meta_data?dtype=string' % ROOT_URL,
            body='OK',
            status=200
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_md_number/3?dtype=number' % ROOT_URL,
            body='OK',
            status=200
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_md_date/1234567890123?dtype=date' % ROOT_URL,
            body='OK',
            status=200
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_md_complex/{key:value}?dtype=complex' % ROOT_URL,
            body='OK',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        # Implicit data type
        result = tdm.import_meta_data(
            tsuid='TSUID',
            name='test_meta_data',
            value='value_of_meta_data')
        self.assertTrue(result)

        # Explicitly specify the data type string
        result = tdm.import_meta_data(
            tsuid='TSUID',
            name='test_meta_data',
            value='value_of_meta_data',
            data_type=DTYPE.string)
        self.assertTrue(result)

        # Explicitly specify the data type number
        result = tdm.import_meta_data(
            tsuid='TSUID',
            name='test_md_number',
            value='3',
            data_type=DTYPE.number)
        self.assertTrue(result)

        # Explicitly specify the data type date
        result = tdm.import_meta_data(
            tsuid='TSUID',
            name='test_md_date',
            value='1234567890123',
            data_type=DTYPE.date)
        self.assertTrue(result)

        # Explicitly specify the data type complex
        result = tdm.import_meta_data(
            tsuid='TSUID',
            name='test_md_complex',
            value='{key:value}',
            data_type=DTYPE.complex)
        self.assertTrue(result)

    @fake_server
    def test_import_md_not_created(self):
        """
        Tests the import of meta data not created
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='OK',
            status=204
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        result = tdm.import_meta_data(tsuid='TSUID', name='test_meta_data', value='value_of_meta_data')
        self.assertFalse(result)

    def test_import_md_wrong_data_type(self):
        """
        Tests the import of meta data with a wrong data type
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        with self.assertRaises(TypeError):
            tdm.import_meta_data(tsuid='TSUID',
                                 name='test_meta_data',
                                 value='value_of_meta_data',
                                 data_type="unknown")

    @fake_server
    def test_import_md_not_exist(self):
        """
        Tests the import of meta data which doesn't exists
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='OK',
            status=404
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.import_meta_data(tsuid='TSUID', name='test_meta_data', value='value_of_meta_data')
        self.assertFalse(results)

    @fake_server
    def test_import_md_force_update(self):
        """
        Tests the creation of meta data which doesn't exists but force update require to create it
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=404
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.PUT,
            '%s/metadata/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.import_meta_data(tsuid='TSUID', name='test_meta_data', value='value_of_meta_data',
                                       force_update=True)
        self.assertFalse(results)

    @fake_server
    def test_update_md(self):
        """
        Tests the update of meta data (nominal)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.PUT,
            '%s/metadata/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.update_meta_data(
            tsuid='TSUID',
            name='test_meta_data',
            value='value_of_meta_data')

        self.assertTrue(results)

    @fake_server
    def test_update_md_not_created(self):
        """
        Tests the update of meta data not created
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.PUT,
            '%s/metadata/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=204
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.update_meta_data(tsuid='TSUID', name='test_meta_data', value='value_of_meta_data')
        self.assertFalse(results)

    @fake_server
    def test_update_md_not_exist(self):
        """
        Tests the update of meta data which doesn't exists
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.PUT,
            '%s/metadata/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=404
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.update_meta_data(tsuid='TSUID', name='test_meta_data', value='value_of_meta_data')
        self.assertFalse(results)

    @fake_server
    def test_update_md_force_create(self):
        """
        Tests the update of meta data which doesn't exists but force create require to create it
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.PUT,
            '%s/metadata/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=404
        )
        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/TSUID/test_meta_data/value_of_meta_data' % ROOT_URL,
            body='',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.update_meta_data(tsuid='TSUID', name='test_meta_data', value='value_of_meta_data',
                                       force_create=True)
        self.assertTrue(results)

    @fake_server
    def test_get_typed_md(self):
        """
        Requests for the meta data associated to a TS with its associated type
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json' % ROOT_URL,
            body="""[{"id":12,"tsuid":"TS1","name":"MD1","value":"my_string", "dtype":"string"},
                 {"id":13,"tsuid":"TS1","name":"MD2","value":"42", "dtype":"number"},
                 {"id":14,"tsuid":"TS1","name":"MD3","value":"1234567890123", "dtype":"date"},
                 {"id":15,"tsuid":"TS1","name":"MD4","value":"{key1:value1}", "dtype":"complex"}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_typed_meta_data('TS1')
        self.assertDictEqual(results, {'TS1': {
            'MD1': {'value': 'my_string', 'type': 'string'},
            'MD2': {'value': '42', 'type': 'number'},
            'MD3': {'value': '1234567890123', 'type': 'date'},
            'MD4': {'value': '{key1:value1}', 'type': 'complex'},
        }})

    @fake_server
    def test_get_md(self):
        """
        Requests for the meta data associated to a TS
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json' % ROOT_URL,
            body="""[{"id":12,"tsuid":"MAM3","name":"cycle","value":"takeoff"},
                 {"id":13,"tsuid":"MAM3","name":"flight","value":"AF2042"},
                 {"id":14,"tsuid":"MAM3","name":"units","value":"meters"}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_meta_data('MAM3')
        self.assertDictEqual(results, {'MAM3': {'cycle': 'takeoff', 'flight': 'AF2042', 'units': 'meters'}})

    @fake_server
    def test_get_md_multi(self):
        """
        Requests for the meta data associated to a TS
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json' % ROOT_URL,
            body="""[{"id":12,"tsuid":"MAM3","name":"cycle","value":"takeoff"},
                 {"id":13,"tsuid":"MAM3","name":"flight","value":"AF2042"},
                 {"id":14,"tsuid":"MAM3","name":"units","value":"meters"},
                 {"id":15,"tsuid":"MAM4","name":"cycle","value":"landing"}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_meta_data(['MAM3', 'MAM4'])

        self.assertDictEqual(results, {
            'MAM3': {
                'cycle': 'takeoff',
                'flight': 'AF2042',
                'units': 'meters'
            },
            'MAM4': {
                'cycle': 'landing'
            },

        })

    @fake_server
    def test_get_md_unknown(self):
        """
        Requests for the meta data associated to an unknown TS
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json' % ROOT_URL,
            body='[]',
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_meta_data('unknown_TS')

        self.assertDictEqual(results, {'unknown_TS': {}})

    @fake_server
    def test_get_md_multi_mixed(self):
        """
        Requests for the meta data associated to several TS having unknown TS
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json' % ROOT_URL,
            body="""[{"id":12,"tsuid":"MAM3","name":"cycle","value":"takeoff"},
                 {"id":13,"tsuid":"MAM3","name":"flight","value":"AF2042"},
                 {"id":14,"tsuid":"MAM3","name":"units","value":"meters"}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_meta_data(['MAM3', 'unknown'])

        self.assertDictEqual(results, {
            'MAM3': {
                'cycle': 'takeoff',
                'flight': 'AF2042',
                'units': 'meters'
            },
            'unknown': {},
        })

    @fake_server
    def test_import_ts_using_array(self):
        """
        Tests the import of data using a numpy array
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/ts/put/test_import_data_using_array' % ROOT_URL,
            body="""
                {
                    "summary": "Import of TS : 00004A000005000AAB",
                    "tsuid":"00004A000005000AAB",
                    "funcId" :"functional_identifier",
                    "errors":{},
                    "numberOfSuccess":6
                }
                """,
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        data = np.array([
            [np.float64(1450772111000), 1.0],
            [np.float64(1450772112000), 2.0],
            [np.float64(1450772113000), 3.0],
            [np.float64(1450772114000), 5.0],
            [np.float64(1450772115000), 8.0],
            [np.float64(1450772116000), 13.0]
        ])
        results = tdm.import_ts_data(metric="test_import_data_using_array",
                                     data=data,
                                     fid="functional_identifier",
                                     tags={'test': 'ok'})

        self.assertEqual(results['numberOfSuccess'], 6)
        self.assertEqual(len(results['errors']), 0)

    @fake_server
    def test_import_ts_without_data_set(self):
        """
        Tests the import of data using a numpy array
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/ts/put/test_import_ts_without_data_set' % ROOT_URL,
            body="""
                {
                "summary": "Import of TS : 00004A000005000AAB",
                "tsuid":"00004A000005000AAB",
                "funcId" : null,
                "errors":{},
                "numberOfSuccess":6
                }
                """,
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        data = np.array([
            [np.float64(1450772111000), 1.0],
            [np.float64(1450772112000), 2.0],
            [np.float64(1450772113000), 3.0],
            [np.float64(1450772114000), 5.0],
            [np.float64(1450772115000), 8.0],
            [np.float64(1450772116000), 13.0]
        ])
        results = tdm.import_ts_data(metric="test_import_ts_without_data_set",
                                     data=data,
                                     fid="functional_identifier",
                                     tags={'test': 'ok'})

        self.assertEqual(results['numberOfSuccess'], 6)
        self.assertEqual(len(results['errors']), 0)

    @fake_server
    def test_import_ts_using_file_path(self):
        """
        Tests the import of data using a file path
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/ts/put/my_metric_name' % ROOT_URL,
            body="""
                {
                    "summary":"Import of TS : 00004A000005000AAB",
                    "tsuid":"00004A000005000AAB",
                    "funcId" :"functional_identifier",
                    "errors":{},
                    "numberOfSuccess":6
                }""",
            status=200,
            content_type='text/json'
        )

        # Create the test file
        with open('/tmp/test.csv', 'w') as opened_file:
            opened_file.write('timestamp;value\n')
            opened_file.write('2015-01-01T00:00:01.0;1\n')
            opened_file.write('2015-01-01T00:00:02.0;2\n')
            opened_file.write('2015-01-01T00:00:03.0;3\n')
            opened_file.write('2015-01-01T00:00:04.0;5\n')
            opened_file.write('2015-01-01T00:00:05.0;8\n')
            opened_file.write('2015-01-01T00:00:06.0;13\n')

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.import_ts_data(metric="my_metric_name",
                                     fid="functional_identifier",
                                     data="/tmp/test.csv",
                                     tags={'tags': 'default'})

        self.assertEqual(results['numberOfSuccess'], 6)
        self.assertEqual(len(results['errors']), 0)

    @fake_server
    def test_import_ts_file_errors(self):
        """
        Tests the import of data using a file path. The file contains errors inside
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/ts/put/my_metric_name' % ROOT_URL,
            body="""
            {
            "summary":"unable to read data from serializer : Unparseable date: '\\"BadDate2015-01-01T00:00:01.0\\"",
            "tsuid":null,
            "funcId" :null,
            "errors":{},
            "numberOfSuccess":0
            }""",
            status=400,
            content_type='text/json'
        )

        # Create the test file
        with open('/tmp/test2.csv', 'w') as opened_file:
            opened_file.write('timestamp;value\n')
            opened_file.write('BadDate2015-01-01T00:00:01.0;1\n')
            opened_file.write('2015-01-01T00:00:02.0;2\n')
            opened_file.write('2015-01-01T00:00:03.0;3\n')
            opened_file.write('2015-01-01T00:00:04.0;5\n')
            opened_file.write('2015-01-01T00:00:05.0;8\n')
            opened_file.write('2015-01-01T00:00:06.0;13\n')

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.import_ts_data(metric="my_metric_name",
                                     fid="functional_identifier",
                                     data="/tmp/test2.csv",
                                     tags={'tags': 'default'})

        # Expected value is correct
        self.assertEqual(results['numberOfSuccess'], 0)
        self.assertEqual(len(results['errors']), 0)
        self.assertFalse(results['status_code'])

    @fake_server
    def test_get_ts_metric_nominal(self):
        """
        Tests the extraction of metric data points (nominal case)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/extract/metric/WS2' % ROOT_URL,
            body="""
                [{"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":8.164285714285715,"1342182768000":0.0}}]
                 """,
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_by_metric("WS2", sd=1325376000000, ed=1342429680000, ds='avg')

        # The previous request shall return points
        self.assertGreater(len(results), 0)
        # The previous request shall return timestamp and value columns
        self.assertEqual(len(results[0]), 2)

    @fake_server
    def test_get_ts_metric_with_tags(self):
        """
        Tests the extraction of metric data points (nominal case)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/extract/metric/WS2' % ROOT_URL,
            body="""[{"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":8.164285714285715,"1342182768000":0.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_by_metric("WS2", sd=1325376000000, ed=1448374667689, dp='7716s', ds='avg',
                                       tags={'aircraftIdentifier': 'A320001'})

        # The previous request shall return points
        self.assertGreater(len(results), 0)
        # The previous request shall return timestamp and value columns
        self.assertEqual(len(results[0]), 2)

    @fake_server
    def test_get_ts_metric_no_ed(self):
        """
        Tests the extraction of metric data points without providing end date
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/extract/metric/WS2' % ROOT_URL,
            body="""[{"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":8.164285714285715,"1342182768000":0.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        # No 'ed'
        results = tdm.get_ts_by_metric("WS2", sd=1325376000000, dp='7716s', ds='avg')

        # The previous request shall return points
        self.assertGreater(len(results), 0)
        # The previous request shall return timestamp and value columns
        self.assertEqual(len(results[0]), 2)

    @fake_server
    def test_get_ts_metric_with_di(self):
        """
        Tests the extraction of metric data points using 'di' flag (to obtain min/max/sd data)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/extract/metric/WS2' % ROOT_URL,
            body="""[{"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":8.164285714285715,"1342182768000":0.0,"1342190484000":0.0,
                 "1342198200000":0.0,"1342205916000":0.5413612565445026,"1342213632000":1.0739106453392169,
                 "1342221348000":0.0,"1342229064000":0.0,"1342236780000":3.2355066535776613}},
                 {"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":23.587409085080523,"1342182768000":0.0,"1342190484000":0.0,
                 "1342198200000":0.0,"1342205916000":6.267769815581672,"1342213632000":5.457486150274969,
                 "1342221348000":0.0,"1342229064000":0.0,"1342236780000":5.31273200424236}},
                 {"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":0.0,"1342182768000":0.0,"1342190484000":0.0,"1342198200000":0.0,
                 "1342205916000":0.0,"1342213632000":0.0,"1342221348000":0.0,"1342229064000":0.0,
                 "1342236780000":0.0}},
                 {"metric":"WS2","tags":{"aircraftIdentifier":"A320001"},"aggregateTags":["flightIdentifier"],
                 "dps":{"1342175052000":159.0,"1342182768000":0.0,"1342190484000":0.0,"1342198200000":0.0,
                 "1342205916000":131.0,"1342213632000":79.0,"1342221348000":0.0,"1342229064000":0.0,
                 "1342236780000":64.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_by_metric("WS2", sd=1325376000000, ed=1342236780000, dp='7716s', ds='avg', di=True)

        # The previous request shall return points
        self.assertGreater(len(results), 0)
        # The previous request shall return timestamp/value/sd/min/max columns
        self.assertEqual(len(results[0]), 5)

    @fake_server
    def test_get_ts_metric_with_no_data(self):
        """
        Tests the extraction of metric data points without using 'di' flag (to obtain min/max/sd data)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/extract/metric/WS2' % ROOT_URL,
            body='[]',
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_by_metric("WS2", sd=1, ed=2)

        # The previous request shall not return points
        self.assertEqual(len(results), 0)

    def test_get_ts_bad_host_value(self):
        """
        Tests robustness of the extraction of metric data points when bad host value is provided
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        # Robustness cases
        with self.assertRaises(ValueError):
            tdm.host = ''

    def test_get_ts_bad_host_type(self):
        """
        Tests robustness of the extraction of metric data points when bad host type is provided
        """
        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        with self.assertRaises(TypeError):
            tdm.host = ()

    def test_get_ts_bad_port_min(self):
        """
        Tests robustness of the extraction of metric data points when port value is out of range (min)
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        with self.assertRaises(ValueError):
            tdm.port = -1

    def test_get_ts_bad_port_max(self):
        """
        Tests robustness of the extraction of metric data points when port value is out of range (max)
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        with self.assertRaises(ValueError):
            tdm.port = 65536

    def test_get_ts_bad_port_format(self):
        """
        Tests robustness of the extraction of metric data points with bad port type (float)
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        with self.assertRaises(TypeError):
            tdm.port = 2.3

    def test_get_ts_bad_port_type(self):
        """
        Tests robustness of the extraction of metric data points with bad port type (str)
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        with self.assertRaises(TypeError):
            tdm.port = '123'

    def test_get_ts_bad_sd_type(self):
        """
        Tests robustness of the extraction of metric data points with bad start date type
        """

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        with self.assertRaises(TypeError):
            # noinspection PyTypeChecker
            tdm.get_ts_by_metric("WS2", sd='not valid sd', ed=1448374667689, dp='7716s', ds='avg')

    @fake_server
    @mock.patch('ikats.core.resource.client.TemporalDataMgr.import_meta_data', import_md_mock)
    def test_get_ts(self):
        """
        Tests the extraction of metric data points without knowing the start date and end date (but meta data contain
        these dates)
        """

        META_DATA_LIST.clear()

        # Fake answer definition

        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json?tsuid=00001600000300077D0000040003F1' % ROOT_URL,
            body="""[{"id":12,"tsuid":"00001600000300077D0000040003F1","name":"ikats_start_date","value":"10000"},
                 {"id":13,"tsuid":"00001600000300077D0000040003F1","name":"flight","value":"AF2042"},
                 {"id":14,"tsuid":"00001600000300077D0000040003F1","name":"ikats_end_date","value":"50000"}]""",
            status=200,
            content_type='text/json'
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            re.compile(".*TemporalDataManagerWebApp/webapi/metadata/import/.*"),
            body='',
            status=200,
        )
        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/metadata/import/00001600000300077D0000040003F1/ikats_end_date/*' % ROOT_URL,
            body='',
            status=200,
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/query' % DIRECT_ROOT_URL,
            body="""[{"tsuid":"00001600000300077D0000040003F1","tags":{"aircraftIdentifier":"A320001"},
                 "aggregateTags":["flightIdentifier"],"dps":{"10000":8.164285714285715,"20000":0.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts("00001600000300077D0000040003F1")

        # The previous request shall return points
        self.assertGreater(len(results), 0)

        # No new meta data stored
        self.assertEqual(META_DATA_LIST, {})

    @fake_server
    @mock.patch('ikats.core.resource.client.TemporalDataMgr.get_meta_data', get_md_mock)
    @mock.patch('ikats.core.resource.client.TemporalDataMgr.import_meta_data', import_md_mock)
    def test_get_ts_without_range(self):
        """
        Tests the extraction of metric data points without knowing the start date and end date (and meta data doesn't
        contain these dates)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json?tsuid=00001600000300077D0000040003F1' % ROOT_URL,
            body="""[{"id":12,"tsuid":"00001600000300077D0000040003F1","name":"label1","value":"1"},
                 {"id":13,"tsuid":"00001600000300077D0000040003F1","name":"flight","value":"AF2042"},
                 {"id":14,"tsuid":"00001600000300077D0000040003F1","name":"label2","value":"2"}]""",
            status=200,
            content_type='text/json'
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            re.compile(".*TemporalDataManagerWebApp/webapi/metadata/import/.*"),
            body='',
            status=200,
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/query' % DIRECT_ROOT_URL,
            body="""[{"tsuid":"00001600000300077D0000040003F1","tags":{"aircraftIdentifier":"A320001"},
                 "aggregateTags":["flightIdentifier"],"dps":{"10000":8.164285714285715,"20000":0.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts("00001600000300077D0000040003F1")

        # The previous request shall return points
        self.assertGreater(len(results), 0)

    @fake_server
    @mock.patch('ikats.core.resource.client.TemporalDataMgr.import_meta_data', import_md_mock)
    def test_get_multi_ts_without_range(self):
        """
        Tests the extraction of metric data points without knowing the start date and end date (and meta data doesn't
        contain these dates) for several ts
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/list/json?tsuid=00001600000300077D0000040003F1' % ROOT_URL,
            body="""[{"id":12,"tsuid":"00001600000300077D0000040003F1","name":"label1","value":"1"},
                 {"id":13,"tsuid":"00001600000300077D0000040003F1","name":"flight","value":"AF2042"},
                 {"id":14,"tsuid":"00001600000300077D0000040003F1","name":"label2","value":"2"}]""",
            status=200,
            content_type='text/json'
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            re.compile(".*TemporalDataManagerWebApp/webapi/metadata/import/.*"),
            body='',
            status=200,
        )

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/query' % DIRECT_ROOT_URL,
            body="""[{"tsuid":"00001600000300077D0000040003F1","tags":{"aircraftIdentifier":"A320001"},
                 "aggregateTags":["flightIdentifier"],"dps":{"10000":8.164285714285715,"20000":0.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts(["00001600000300077D0000040003F1", "00001600000300077D0000040003F2"])

        # The previous request shall return points
        self.assertGreater(len(results), 0)

        self.assertEqual(len(META_DATA_LIST), 2)
        self.assertEqual(len(META_DATA_LIST['00001600000300077D0000040003F1']), 3)
        self.assertEqual(len(META_DATA_LIST['00001600000300077D0000040003F2']), 3)
        self.assertEqual(META_DATA_LIST['00001600000300077D0000040003F1']['ikats_start_date']['value'], 10000)
        self.assertEqual(META_DATA_LIST['00001600000300077D0000040003F1']['ikats_end_date']['value'], 20000)
        self.assertEqual(META_DATA_LIST['00001600000300077D0000040003F1']['qual_nb_points']['value'], 2)
        self.assertEqual(META_DATA_LIST['00001600000300077D0000040003F2']['ikats_start_date']['value'], 10000)
        self.assertEqual(META_DATA_LIST['00001600000300077D0000040003F2']['ikats_end_date']['value'], 20000)
        self.assertEqual(META_DATA_LIST['00001600000300077D0000040003F2']['qual_nb_points']['value'], 2)

    @fake_server
    def test_get_ts_by_tsuid(self):
        """
        Tests the extraction of metric data points using the TSUID information as key
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/query' % DIRECT_ROOT_URL,
            body="""[{"metric":"WS6","tags":{"flightIdentifier":"90999","aircraftIdentifier":"A320001"},
                 "aggregateTags":[],"dps":{"1343720805":0.0,"1343729781":0.0}}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_by_tsuid("00001600000300077D0000040003F1", sd=1343720805, ed=1343729781)

        # The previous request shall return points
        self.assertGreater(len(results), 0)

    @fake_server
    def test_get_ts_no_data(self):
        """
        Tests the extraction of metric data points without using 'di' flag (to obtain min/max/sd data)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/query' % DIRECT_ROOT_URL,
            body='[]',
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_by_tsuid("00001600000300077D0000040003F1", sd=1, ed=2)

        # The previous request shall not return points
        self.assertEqual(len(results), 0)

    @fake_server
    def test_dataset_create(self):
        """
        Tests the import of a data set (nominal)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            '%s/dataset/import/id_of_data_set' % ROOT_URL,
            body='OK',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.dataset_create('id_of_data_set', 'description of my data set', ['TSUID1', 'TSUID2', 'TSUID3'])

        self.assertTrue(results)

    @fake_server
    def test_get_ts_info(self):
        """
        Tests the gathering of the annotation of a TS (nominal)
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/lookup/WS1' % ROOT_URL,
            body="""{"type":"LOOKUP","metric":"WS1","tags":[],"limit":25,"time":48.0,"results":
                 [{"tags":{"flightIdentifier":"1","aircraftIdentifier":"A320001"},
                 "tsuid":"0000110000030003F20000040003F1","metric":"WS1"},
                 {"tags":{"flightIdentifier":"2","aircraftIdentifier":"A320001"},
                 "tsuid":"0000110000030003F30000040003F1","metric":"WS1"}],"startIndex":0,"totalResults":380}""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_info("WS1")

        self.assertGreater(len(results), 0)

    @fake_server
    def test_get_ts_info_unknown(self):
        """
        Tests the gathering of the annotation of an unknown TS
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/lookup/unknownTS' % ROOT_URL,
            body='{"error":{"code":404,"message":"Unable to resolve one or more names","details":"---"}}',
            status=404,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_info("unknownTS")

        self.assertEqual(results, [])

    @fake_server
    def test_get_data_set_nominal(self):
        """
        Request for a data set which exists in Ikats
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/dataset/id_of_data_set' % ROOT_URL,
            body="""{"name":"id_of_data_set","description":"description of my data set","tsuids":
                 [{"tsuid":"TSUID1"},{"tsuid":"TSUID2"},{"tsuid":"TSUID3"}],
                 "tsuidsAsString":["TSUID1","TSUID2","TSUID3"]}""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        dataset = tdm.get_data_set('id_of_data_set')
        self.assertEqual(dataset['ts_list'], ['TSUID1', 'TSUID2', 'TSUID3'])
        self.assertEqual(dataset['description'], 'description of my data set')

    @fake_server
    def test_get_data_set_unknown(self):
        """
        Request for a data set which doesn't exist in Ikats
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/dataset/unknown_data_set' % ROOT_URL,
            body='dataset with id : unknown_data_set not found on server',
            status=404
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        dataset = tdm.get_data_set('unknown_data_set')

        self.assertEqual(dataset['ts_list'], [])
        self.assertEqual(dataset['description'], None)

    @fake_server
    def test_remove_ts(self):
        """
        Request to remove an existing timeseries
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.DELETE,
            '%s/ts/tsuid' % ROOT_URL,
            body='Deletion of timeseries tsuid OK',
            status=204
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        tdm.remove_ts('tsuid')

    @fake_server
    def test_remove_data_set(self):
        """
        Request to remove an existing data set
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.DELETE,
            '%s/dataset/id_of_data_set' % ROOT_URL,
            body='Deletion of dataSet id_of_data_set OK',
            status=200
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        status = tdm.dataset_delete('id_of_data_set')
        self.assertTrue(status)

    @fake_server
    def test_remove_data_set_unknown(self):
        """
        Trying to remove an unknown data set
        No difference with removing an existing data set
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.DELETE,
            '%s/dataset/unknown data set' % ROOT_URL,
            body='Deletion of dataSet unknown data set OK',
            # Even if the data_set is unknown, the deletion operation is a success
            status=200
        )
        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        status = tdm.dataset_delete('unknown data set')

        self.assertTrue(status)

    @fake_server
    def test_get_ts_from_md_empty(self):
        """
        Get a TS from empty meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/tsmatch' % ROOT_URL,
            body='["TS1", "TS2", "TS3"]',
            # Even if the data_set is unknown, the operation is a success
            status=200,
            content_type='application/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_ts_from_meta_data()

        self.assertGreater(len(results), 0)

    @fake_server
    def test_get_ts_from_md_with_param(self):
        """
        Get a TS from valid meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/tsmatch' % ROOT_URL,
            body='["TS1","TS2","TS3"]',
            # Even if the data_set is unknown, the operation is a success
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_ts_from_meta_data({'cycle': ['takeoff', 'landing']})

        self.assertGreater(len(results), 0)

    @fake_server
    def test_get_ts_from_md_no_results(self):
        """
        Get a TS from valid meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/metadata/tsmatch' % ROOT_URL,
            body='[]',
            # Even if the data_set is unknown, the operation is a success
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_ts_from_meta_data({'unknown_meta': '42'})

        self.assertEqual(len(results), 0)

    @fake_server
    def test_get_ts_list(self):
        """
        Get a TS from valid meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/tsuid' % ROOT_URL,
            body="""[{"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000002"},
                 {"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000003"},
                 {"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000004"},
                 {"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000005"},
                 {"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000006"},
                 {"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000007"},
                 {"tsuid":"poc.sinusoide.outliers","metric":"000001000001000001000002000008"}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_ts_list()

        self.assertEqual(len(results), 7)

    @fake_server
    def test_get_ts_list_empty(self):
        """
        Get a TS from valid meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/tsuid' % ROOT_URL,
            body='[]',
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.get_ts_list()

        self.assertEqual(len(results), 0)

    @fake_server
    def test_get_ts_meta(self):
        """
        Tests the extraction of tsuid metadata with TSUID information as key
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/ts/tsuid/%s' % (ROOT_URL, '00001600000300077D0000040003F1'),
            body="""
                {"tsuid":"00001600000300077D0000040003F1",
                "funcId":"A320001_90999_WS6",
                "metric":"WS6",
                "tags":{"flightIdentifier":"90999","aircraftIdentifier":"A320001"}}
            """,
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)

        results = tdm.get_ts_meta("00001600000300077D0000040003F1")

        # The previous request shall return points
        self.assertGreater(len(results), 0)

    @fake_server
    def test_get_dataset_list(self):
        """
        Get a TS from valid meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/dataset' % ROOT_URL,
            body="""[{"name":"dataset_MAM1","description":"1st dataset"},
                 {"name":"datasetMAM1","description":"1st dataset"},
                 {"name":"test_data_PHC","description":"test for PHC"}]""",
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.dataset_list()

        self.assertEqual(len(results), 3)

    @fake_server
    def test_get_dataset_list_empty(self):
        """
        Get a TS from valid meta data
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            '%s/dataset' % ROOT_URL,
            body='[]',
            status=200,
            content_type='text/json'
        )

        tdm = TemporalDataMgr(TEST_HOST, TEST_PORT)
        results = tdm.dataset_list()

        self.assertEqual(len(results), 0)


if __name__ == '__main__':
    unittest.main()

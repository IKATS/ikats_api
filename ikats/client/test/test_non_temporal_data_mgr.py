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
from unittest import TestCase

from ikats.core.config.ConfigReader import ConfigReader
from ikats.client import NonTemporalDataMgr
import httpretty

# Flag to set to True to use the real servers (setting it to False will use a fake local server)
USE_REAL_SERVER = False

LOGGER = logging.getLogger('ikats.core.resource.client.rest_client')

# Defines the log level to DEBUG
LOGGER.setLevel(logging.DEBUG)

# Log format
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')

# Create another handler that will redirect log entries to STDOUT
STREAM_HANDLER = logging.StreamHandler()
STREAM_HANDLER.setLevel(logging.DEBUG)
STREAM_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(STREAM_HANDLER)

# Address of the real server to use for tests
CONFIG_READER = ConfigReader()
TEST_HOST = CONFIG_READER.get('cluster', 'client.ip')
TEST_PORT = int(CONFIG_READER.get('cluster', 'client.port'))

# Disable real connection depending on the usage of real or fake server
httpretty.HTTPretty.allow_net_connect = USE_REAL_SERVER


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


class TestNonTemporalDataMgr(TestCase):
    """
    Tests the Manager API for Non Temporal Data
    """

    @fake_server
    def test_add_data_csv(self):
        """
        Test add process data into
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            'http://%s:%s/TemporalDataManagerWebApp/webapi/processdata/exec4' % (TEST_HOST, TEST_PORT),
            body='OK',
            status=200
        )
        ntdm = NonTemporalDataMgr()
        # Create the test file
        with open('/tmp/test.csv', 'w') as opened_file:
            opened_file.write('timestamp;value\n')
            opened_file.write('2015-01-01T00:00:01.0;1\n')
            opened_file.write('2015-01-01T00:00:02.0;2\n')
            opened_file.write('2015-01-01T00:00:03.0;3\n')
            opened_file.write('2015-01-01T00:00:04.0;5\n')
            opened_file.write('2015-01-01T00:00:05.0;8\n')
            opened_file.write('2015-01-01T00:00:06.0;13\n')
        results = ntdm.add_data("/tmp/test.csv", "exec4", "CSV")
        if not results['status_code']:
            self.fail()

    @fake_server
    def test_add_data_json(self):
        """
        Test add process data into
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            'http://%s:%s/TemporalDataManagerWebApp/webapi/processdata/portfolio_interpolation/JSON' % (
                TEST_HOST, TEST_PORT),
            body='OK',
            status=200
        )
        ntdm = NonTemporalDataMgr()

        results = ntdm.add_data(
            str(["00006100000B005FC4", "00006200000B005FCB", "00006300000B005FCE", "00006400000B005FC6",
                 "00006500000B005FC7", "00006600000B005FD0", "00006700000B005FCF", "00006900000B005FC5",
                 "00006A00000B005FC9", "00006B00000B005FCA", "00006C00000B005FCC", "00006D00000B005FCD",
                 "00006E00000B005FC8"]),
            "portfolio_interpolation", "JSON", "JSON_name")

        if not results['status_code']:
            self.fail()

    @fake_server
    def test_add_data_any(self):
        """
        Test add process data into
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.POST,
            'http://%s:%s/TemporalDataManagerWebApp/webapi/processdata' % (TEST_HOST, TEST_PORT),
            body='12',
            status=200
        )

        ntdm = NonTemporalDataMgr()

        data_to_send = "Any opaque data"

        results = ntdm.add_data(data=data_to_send, data_type=None, name="Name_of_data", process_id=42)

        if not results['status_code']:
            self.fail()

    @staticmethod
    @fake_server
    def test_download_data():
        """
        Not implemented
        """

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            'http://%s:%s/TemporalDataManagerWebApp/webapi/processdata/id/download/1' % (TEST_HOST, TEST_PORT),
            status=200
        )
        ntdm = NonTemporalDataMgr()
        ntdm.download_data("1")

    @staticmethod
    @fake_server
    def test_remove_data():
        """
        Not implemented
        """
        ntdm = NonTemporalDataMgr()
        # Fake answer definition
        httpretty.register_uri(
            httpretty.DELETE,
            'http://%s:%s/TemporalDataManagerWebApp/webapi/processdata/exec4' % (TEST_HOST, TEST_PORT),
            status=204,
            content_type='text/json'
        )

        ntdm.remove_data("exec4")

    @staticmethod
    @fake_server
    def test_get_data():
        """
        Requests for the meta data associated to a TS
        """
        ntdm = NonTemporalDataMgr()

        # Fake answer definition
        httpretty.register_uri(
            httpretty.GET,
            'http://%s:%s/TemporalDataManagerWebApp/webapi/processdata/exec4' % (TEST_HOST, TEST_PORT),
            body='[{"id":1,"processId":"exec4","dataType":"CSV","name":"distance_matrix.csv"}]',
            status=200,
            content_type='text/json'
        )

        ntdm.get_data("exec4")

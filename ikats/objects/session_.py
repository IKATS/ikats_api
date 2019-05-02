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
import logging
import os
import requests

from ikats.lib import check_type


class IkatsSession:
    """
    IkatsSession is the connector to IKATS resources.
    It provides a way for other IKATS class to know where the data are.
    The IKATS entry point shall be set to the main GUI URL and port.
    """

    def __init__(self, host=None, port="80", sc=None, name="IKATS_SESSION"):
        """
        Initialize the session

        :param host: URL or IP to the GUI
        :param port: Port of the GUI
        :param sc: Spark context if exists
        :param name: Name of the session (in the case you manage several session

        :type host: str
        :type port: str or int
        :type sc: SparkContext or SparkSession
        :type name: str
        """

        # Host name and port of the GUI
        self.__host = None
        self.__port = None

        # URL to backends REST API
        self.__catalog_url = None
        self.__engine_url = None
        self.__dm_url = None
        self.__tsdb_url = None

        # Spark Context/Session
        self.__sc = None

        # Requests Session
        self.__rs = requests.session()

        # Initialization
        self.host = host
        self.port = port
        self.sc = sc

        self.catalog_url = "/pybase"
        self.engine_url = "/pybase"
        self.dm_url = "/datamodel"
        self.tsdb_url = "/tsdb"

        self.name = name
        self.log = logging.getLogger(str(self.name))
        self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(logging.DEBUG)

        # Set the requests modules minimum logger to Warning
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    @property
    def rs(self):
        """
        requests Session
        :rtype: requests.Session
        """
        return self.__rs

    @rs.setter
    def rs(self, value):
        check_type(value=value, allowed_types=requests.Session, var_name="rs", raise_exception=True)
        self.__rs = value

    @property
    def host(self):
        """
        Hostname of the IKATS backend
        :rtype: str
        """
        return self.__host

    @host.setter
    def host(self, value):

        # Default value is localhost, overridden by environment variable IKATS_GUI_HOST, overridden by explicit value
        if value is None:
            value = os.environ.get("IKATS_GUI_HOST", "http://localhost")
            if not value.startswith("http"):
                value="http://%s"%value

        check_type(value=value, allowed_types=str, var_name="host", raise_exception=True)
        self.__host = value

    @property
    def port(self):
        """
        Port of the backend
        :rtype: int
        """
        return self.__port

    @port.setter
    def port(self, value):

        # Default value is localhost, overridden by environment variable IKATS_GUI_HOST, overridden by explicit value
        if value is None:
            value = int(os.environ.get("IKATS_GUI_PORT", 80))

        check_type(value=value, allowed_types=[int, str, float, None], var_name="port", raise_exception=True)

        if int(value) <= 0 or int(value) >= 65535:
            raise ValueError("Port must be within ]0;65535] (got %s)" % value)

        self.__port = int(value)

    @property
    def dm_url(self):
        """
        URL of the Datamodel API
        :rtype: str
        """
        return self.__dm_url

    @dm_url.setter
    def dm_url(self, value):
        self.__dm_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def tsdb_url(self):
        """
        URL of the Timeseries database
        :rtype: str
        """
        return self.__tsdb_url

    @tsdb_url.setter
    def tsdb_url(self, value):
        self.__tsdb_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def engine_url(self):
        """
        URL of the Operator runner engine
        :rtype: str
        """
        return self.__engine_url

    @engine_url.setter
    def engine_url(self, value):

        self.__engine_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def catalog_url(self):
        """
        URL of the Catalog backend
        :rtype: str
        """
        return self.__catalog_url

    @catalog_url.setter
    def catalog_url(self, value):
        self.__catalog_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def sc(self):
        """
        Spark Context
        :rtype: SparkContext
        """
        return self.__sc

    @sc.setter
    def sc(self, value):
        self.__sc = value

    def __repr__(self):
        return str("IKATS session to {}:{}".format(self.host, self.port))

    def __str__(self):
        return self.__repr__()

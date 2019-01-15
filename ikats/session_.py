#!/bin/python3
import logging
import re

import requests

from ikats.lib import check_type


class IkatsSession:
    """
    IkatsSession is the connector to IKATS resources
    It provides a way for other IKATS class to know where the data are
    The IKATS entry point shall be set to the main GUI URL and port.
    """

    def __init__(self, host="http://localhost", port="80", sc=None, name="IKATS_SESSION"):
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
        self.__tdm_url = None
        self.__tsdb_url = None

        # Spark Context/Session
        self.__sc = None

        # Requests Session
        self.__rs = requests.session()

        # Initialization
        self.host = host
        self.port = port
        self.sc = sc

        self.catalog_url = "/python_api"
        self.engine_url = "/pybase/ikats/algo/execute"
        self.tdm_url = "/datamodel-api"
        self.tsdb_url = "/opentsdb"

        self.name = name
        self.log = logging.getLogger(str(self.name))
        self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(logging.DEBUG)

    @property
    def rs(self):
        """requests Session"""
        return self.__rs

    @rs.setter
    def rs(self, value):
        check_type(value=value, allowed_types=requests.Session, var_name="rs", raise_exception=True)
        self.__rs = value

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, value):
        regex = re.compile(
            r'^(?:http)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # Domain
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ... or IP
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        if re.match(regex, value) is not None:
            self.__host = str(value)
        else:
            raise ValueError("Malformed host name: %s" % value)

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, value):
        check_type(value=value, allowed_types=[int, str, float, None], var_name="port", raise_exception=True)

        if int(value) <= 0 or int(value) >= 65535:
            raise ValueError("Port must be within ]0;65535] (got %s)" % value)

        self.__port = int(value)

    @property
    def tdm_url(self):
        return self.__tdm_url

    @tdm_url.setter
    def tdm_url(self, value):
        self.__tdm_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def tsdb_url(self):
        return self.__tsdb_url

    @tsdb_url.setter
    def tsdb_url(self, value):
        self.__tsdb_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def engine_url(self):
        return self.__engine_url

    @engine_url.setter
    def engine_url(self, value):

        self.__engine_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def catalog_url(self):
        return self.__catalog_url

    @catalog_url.setter
    def catalog_url(self, value):
        self.__catalog_url = "{}:{}{}".format(self.host, self.port, value)

    @property
    def sc(self):
        return self.__sc

    @sc.setter
    def sc(self, value):
        self.__sc = value

    def __repr__(self):
        return str("IKATS session to {}:{}".format(self.host, self.port))

    def __str__(self):
        return self.__repr__()

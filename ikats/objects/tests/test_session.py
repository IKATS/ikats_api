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

from unittest import TestCase

import requests

from ikats.objects.session_ import IkatsSession


class TestSession(TestCase):
    """
    Tests the session class
    """
    def test_nominal(self):
        """
        Test Session nominal usages
        """
        # Default session
        session = IkatsSession()
        self.assertEqual("http://localhost", session.host)
        self.assertEqual(80, session.port)

        self.assertEqual("%s:%s/pybase" % (session.host, session.port), session.catalog_url)
        self.assertEqual("%s:%s/pybase" %
                         (session.host, session.port), session.engine_url)
        self.assertEqual("%s:%s/datamodel" % (session.host, session.port), session.dm_url)
        self.assertEqual("%s:%s/tsdb" % (session.host, session.port), session.tsdb_url)
        self.assertEqual(requests.Session, type(session.rs))

        # Nominal session
        host = "http://localhost"
        port = 80
        session = IkatsSession(host=host, port=port)
        self.assertEqual(host, session.host)
        self.assertEqual(port, session.port)

        # Other nominal case implying port as string and URL with scheme
        session = IkatsSession(host='http://ikats.org', port="80")
        self.assertEqual("http://ikats.org", session.host)
        self.assertEqual(80, session.port)

    def test_malformed_host(self):
        """
        Test Session non-nominal usages
        """
        # Space in URL
        with self.assertRaises(ValueError):
            IkatsSession(host="space in url")

        # No scheme
        with self.assertRaises(ValueError):
            IkatsSession(host="ikats.org")

        # No URL
        with self.assertRaises(ValueError):
            IkatsSession(host="https://")

        # Bad IP
        with self.assertRaises(ValueError):
            IkatsSession(host="https://1.2.3.4.5")

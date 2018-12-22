from unittest import TestCase

import requests

from ..session_ import IkatsSession


class TestSession(TestCase):
    def test_new(self):
        # Default session
        s = IkatsSession()
        self.assertEqual("http://localhost", s.host)
        self.assertEqual(80, s.port)

        self.assertEqual("%s:%s/pybase/ikats/algo/catalogue/" % (s.host, s.port), s.catalog_url)
        self.assertEqual("%s:%s/pybase/ikats/algo/execute/" % (s.host, s.port), s.engine_url)
        self.assertEqual("%s:%s/datamodel-api/TemporalDataManagerWebApp/webapi/" % (s.host, s.port), s.tdm_url)
        self.assertEqual("%s:%s/opentsdb/" % (s.host, s.port), s.tsdb_url)
        self.assertEqual(requests.Session, type(s.rs))

        # Nominal session
        host = "http://localhost"
        port = 80
        s = IkatsSession(host=host, port=port)
        self.assertEqual(host, s.host)
        self.assertEqual(port, s.port)

        # Other nominal case implying port as string and URL with scheme
        s = IkatsSession(host='http://ikats.org', port="80")
        self.assertEqual("http://ikats.org", s.host)
        self.assertEqual(80, s.port)

    def test_malformed_host(self):
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

    def test_connection(self):
        """
        Embedded connection validator
        """
        s = IkatsSession()
        self.assertTrue(s.test_connection())
        s = IkatsSession(host="http://unknownhost.com")
        self.assertFalse(s.test_connection())

    def test_instances(self):
        self.assertEqual(0, len(IkatsSession.instances))
        s1 = IkatsSession()
        self.assertEqual(1, len(IkatsSession.instances))
        s2 = IkatsSession()
        self.assertEqual(2, len(IkatsSession.instances))
        del s1
        self.assertEqual(2, len(IkatsSession.instances))


from unittest import TestCase

from ikats.api import IkatsAPI
from ikats.objects.metadata_ import DTYPE


class TestMetadata(TestCase):

    def test_types(self):
        """
        Creation of a Metadata instance
        """

        # Init
        api = IkatsAPI()
        ts = api.ts.new(tsuid="ABCD")

        # Provide a number, get a string
        ts.metadata.set(name="myMD", value=42, dtype=DTYPE.STRING)
        self.assertEqual("42", ts.metadata.get(name="myMD"))
        self.assertEqual(str, type(ts.metadata.get(name="myMD")))

        # Provide an int as string, get an int
        ts.metadata.set(name="myMD", value="42", dtype=DTYPE.NUMBER)
        self.assertEqual(int, type(ts.metadata.get(name="myMD")))

        # Provide an float as string, get a float
        ts.metadata.set(name="myMD", value="42.5", dtype=DTYPE.NUMBER)
        self.assertEqual(float, type(ts.metadata.get(name="myMD")))

        # Provide a date as string, get an int
        ts.metadata.set(name="myMD", value="42", dtype=DTYPE.DATE)
        self.assertEqual(42, ts.metadata.get(name="myMD"))

        # Provide a date as int, get an int
        ts.metadata.set(name="myMD", value=1564856, dtype=DTYPE.DATE)
        self.assertEqual(1564856, ts.metadata.get(name="myMD"))

    def test_save(self):
        # Init
        api = IkatsAPI()
        ts = api.ts.new(tsuid="ABCD")
        ts.metadata.set(name="myMD", value=42, dtype=DTYPE.STRING)

        self.assertTrue(ts.metadata.save())

        ts2 = api.ts.new(tsuid="ABCD")
        self.assertEqual("42", ts2.metadata.get("myMD"))

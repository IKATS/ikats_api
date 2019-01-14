from unittest import TestCase

from ikats.api import IkatsAPI
from ikats.objects.metadata_ import DTYPE
from ikats.objects.tests.lib import delete_ts_if_exists


class TestMetadata(TestCase):

    def test_types(self):
        """
        Creation of a Metadata instance
        """

        # Init
        api = IkatsAPI()
        delete_ts_if_exists("MyTS")
        ts = api.ts.new(fid="MyTS")

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

        ts.delete()

    def test_nominal(self):
        # Init
        api = IkatsAPI()
        delete_ts_if_exists(fid="MyTS")
        ts_1 = api.ts.new(fid="MyTS")

        # Create new MD
        ts_1.metadata.set(name="myMD", value=42, dtype=DTYPE.STRING)

        # Save it
        self.assertTrue(ts_1.metadata.save())

        # In a new object, get it
        ts_2 = api.ts.get(fid="MyTS")
        self.assertEqual("42", ts_2.metadata.get("myMD"))

        # Mark deleted in first object
        ts_1.metadata.delete(name="myMD")

        # MD not present in first object
        with self.assertRaises(ValueError):
            ts_1.metadata.get("myMD")

        # MD still present in second object (first not synced with database)
        self.assertEqual("42", ts_2.metadata.get("myMD"))

        # Save it
        self.assertTrue(ts_1.metadata.save())

        # MD still present in second object (first not synced with database)
        self.assertEqual("42", ts_2.metadata.get("myMD"))

        # Get last updates
        ts_2.metadata.fetch()

        # MD not present in second object
        with self.assertRaises(ValueError):
            ts_2.metadata.get("myMD")

        # MD not present in just created object
        ts_3 = api.ts.get(fid="MyTS")
        with self.assertRaises(ValueError):
            ts_3.metadata.get("myMD")

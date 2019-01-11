from unittest import TestCase

from ikats.extra.timeseries import gen_random_ts


class TestDataset(TestCase):
    def test_gen_random_ts(self):
        sd = 1000000000000
        ed = 1000000010000
        nb_points = 10
        period = 1000

        # Provide all except period
        data = gen_random_ts(sd=sd, ed=ed, nb_points=nb_points)
        self.assertEqual(sd, data[0][0])
        self.assertEqual(ed - period, data[-1][0])
        self.assertEqual(nb_points, len(data))
        self.assertEqual(period, data[1][0] - data[0][0])

        # Provide all except nb_points
        data = gen_random_ts(sd=sd, ed=ed, period=period)
        self.assertEqual(sd, data[0][0])
        self.assertEqual(ed - period, data[-1][0])
        self.assertEqual(nb_points, len(data))
        self.assertEqual(period, data[1][0] - data[0][0])

        # Provide all except ed
        data = gen_random_ts(sd=sd, nb_points=nb_points, period=period)
        self.assertEqual(sd, data[0][0])
        self.assertEqual(ed - period, data[-1][0])
        self.assertEqual(nb_points, len(data))
        self.assertEqual(period, data[1][0] - data[0][0])

        # Provide all except sd
        data = gen_random_ts(ed=ed, nb_points=nb_points, period=period)
        self.assertEqual(sd, data[0][0])
        self.assertEqual(ed - period, data[-1][0])
        self.assertEqual(nb_points, len(data))
        self.assertEqual(period, data[1][0] - data[0][0])

        # Provide all
        data = gen_random_ts(sd=sd, ed=ed, nb_points=nb_points, period=period)
        self.assertEqual(sd, data[0][0])
        self.assertEqual(ed - period, data[-1][0])
        self.assertEqual(nb_points, len(data))
        self.assertEqual(period, data[1][0] - data[0][0])

        # Mismatch between parameters
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=ed, nb_points=nb_points, period=42)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=ed, nb_points=42, period=period)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=42, nb_points=nb_points, period=period)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=42, ed=ed, nb_points=nb_points, period=period)

        # Not aligned points (period not aligned with end date)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=ed, period=42)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=ed, nb_points=42)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=sd+18, nb_points=42)

        # Missing values
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, ed=ed)
        with self.assertRaises(ValueError):
            gen_random_ts(sd=sd, period=period)

import random

from ikats.objects.timeseries_ import Timeseries


def gen_random(sd=None, ed=None, nb_points=None, period=None):
    """
    Generates a random Timeseries composed of nb_points between sd and ed (start & end dates)
    sd, ed and nb_points are optional.


    :param sd: start date (in ms since EPOCH)
    :param ed: end date (in ms since EPOCH)
    :param nb_points: number of points
    :param period: difference between successive points (in ms)

    :type sd: int
    :type ed: int
    :type nb_points: int
    :type period: int

    :return: the Timeseries object (only local, not saved)
    :rtype: Timeseries
    """

    # At least 3 out of 4 parameters shall be set in order to create the data
    if len([x for x in [sd, ed, nb_points, period] if x is not None]) < 3:
        raise ValueError("Missing inputs, can't generate Timeseries")

    if nb_points is None:
        nb_points = int((ed - sd) / period)
    if period is None:
        period = int((ed - sd) / nb_points)
    if sd is None:
        sd = ed - (nb_points * period)
    if ed is None:
        ed = (nb_points * period) - sd

    # check consistency
    if int((ed - sd) / period) != nb_points or not ((ed - sd) / period).is_integer():
        raise ValueError("Bad inputs, can't generate Timeseries")

    # generate data
    time_col = range(sd, ed, period)
    val_col = [random.random() * 10 - 5]
    for _ in time_col:
        val_col.append(random.random() * 10 - 5 + val_col[-1])
    return list(zip(time_col, val_col))


print(gen_random(sd=1000, ed=10000, period=1000, nb_points=9))

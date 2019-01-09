from ikats.objects.timeseries_ import Timeseries


def gen_random(api, fid, sd=1000000000000, ed=1000500000000, nb_points=500000, period=1000):
    """
    Generates a random Timeseries composed of nb_points between sd and ed (start & end dates)
    sd, ed and nb_points are optional.


    :param fid: name
    :param sd: start date (in ms since EPOCH) - default: 1000000000000
    :param ed: end date (in ms since EPOCH) - default: 1000500000000
    :param nb_points: number of points - default: 500000
    :param period: difference between successive points (in ms) - default: 1000

    :type fid: str
    :type sd: int
    :type ed: int
    :type nb_points: int
    :type period: int

    :return: the Timeseries object (only local, not saved)
    :rtype: Timeseries
    """

    data = range(10)
    return Timeseries(fid=fid, api=api, data=data)

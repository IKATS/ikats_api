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

import random


def gen_random_ts(sd=None, ed=None, nb_points=None, period=None):
    """
    Generates a random Timeseries composed of nb_points between sd and ed (start & end dates)
    end_date is excluded from the range, ie. [sd;ed[


    :param sd: start date (in ms since EPOCH)
    :param ed: end date (in ms since EPOCH)
    :param nb_points: number of points
    :param period: difference between successive points (in ms)

    :type sd: int
    :type ed: int
    :type nb_points: int
    :type period: int

    :returns: the data points in a 2D array where 1st col is the timestamp in EPOCH (ms) and the 2nd is the value
    :rtype: list of points
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
        ed = sd + (nb_points * period)

    # check consistency
    if period == 0 or int((ed - sd) / period) != nb_points or not ((ed - sd) / period).is_integer():
        raise ValueError("Bad inputs, can't generate Timeseries")

    # generate data
    time_col = range(sd, ed, period)
    val_col = [random.random() * 10 - 5]
    for _ in time_col:
        val_col.append(random.random() * 10 - 5 + val_col[-1])
    return list(zip(time_col, val_col))

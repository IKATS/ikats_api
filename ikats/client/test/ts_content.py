"""
Copyright 2018 CS Systèmes d'Information

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
from ikats.core.library.exception import IkatsException
from numpy import concatenate as np_concatenate
from numpy import delete as np_delete
from numpy import empty as np_empty
from numpy import arange as np_arange
from numpy import random as np_random


class TsManagement(object):
    """
    High level TS management
    """

    @staticmethod
    def contains(ts, timestamp, value):
        """
        mock
        no matter if not optimized ... done for the merge

        :param ts:
        :param timestamp:
        :param value:
        :return:
        """
        for point in ts:
            if (point[0] == timestamp) and (point[1] == value):
                return True
        return False

    @staticmethod
    def select_period(ts, sd=None, ed=None):
        """
        Returns the selected period in the timeseries

        :param ts: timeseries data
        :param sd: start date
        :param ed: end date
        :return: the filtered data
        """
        try:

            # detect unconstrained criteria
            sd_undefined = sd is None
            ed_undefined = ed is None

            if sd_undefined and ed_undefined:
                return ts

            # selected is True when current point is in the selected period
            selected = False
            i_first = -1
            i_last = -1
            curr_index = 0

            for timestamp in ts[:, 0]:

                if selected is False:
                    if (sd_undefined or (timestamp >= sd)) and (ed_undefined or (timestamp <= ed)):
                        selected = True
                        i_first = curr_index
                        i_last = i_first
                        if ed_undefined is True:
                            # it is useless to go on: all next indexes are selected
                            i_last = len(ts) - 1
                            break
                elif selected is True:
                    if ed_undefined or (timestamp <= ed):
                        i_last = curr_index

                curr_index = curr_index + 1

            if (i_first == -1) or (i_last == -1):
                # empty array
                return ts[0:0]
            else:
                i_last = i_last + 1
                if i_last == len(ts):
                    return ts[i_first:]
                else:
                    return ts[i_first:i_last]

        except Exception:
            raise IkatsException("Error occurred in select_period({},{},{})".format(ts, sd, ed))

    @staticmethod
    def sort(ts):
        """
        Sorting the ts points according to the timestamp field
        :param ts:
        :type ts:
        :return: ts sorted by timestamps
        :rtype: numpy array
        """
        # Seen in  http://stackoverflow.com/questions/2828059/sorting-arrays-in-numpy-by-column
        # several methods ...
        #  chosen simplest+fastest one
        return ts[ts[:, 0].argsort()]

    @staticmethod
    def merge(ts_one, ts_two):
        """
        Makes the union of points.
        In case of duplicate points - ie sharing same timestamp -: we keep the point from ts_two

        Note: this method performance must be improved in operational context,
        when there are a lot of duplicate timestamps.
        But it is OK for unittest.

        :param ts_one:
        :type ts_one:
        :param ts_two:
        :type ts_two:
        :return: the ts sorted by timestamps, without duplicate timestamps
        :rtype: numpy array
        """
        # concatenate ts_one with ts_two and sort them by timestamp
        points_merged = TsManagement.sort(np_concatenate((ts_one, ts_two), axis=0))

        # ... some duplicates still exist: ought to be removed
        # will never exist: negative int
        prev_time = -1
        # just preceding zero: required ...
        index = -1
        duplicates_deleted_index = []
        for point in points_merged:
            # at the moment: index stands for previous index
            # and when -1 => before first point
            curr_time = point[0]
            # in case of duplicates detection: add the previous index
            # to duplicates_deleted_index
            if curr_time == prev_time:
                # !!! here perfo may be improved: but it is ok for mock context !!!
                if TsManagement.contains(ts_two, curr_time, point[1]):
                    # keep current point because it belongs to ts_two
                    duplicates_deleted_index.append(index)
                else:
                    # remove current point because it belongs to ts_one
                    duplicates_deleted_index.append(index + 1)
            prev_time = curr_time
            # skipping to
            index = index + 1

        return np_delete(points_merged, duplicates_deleted_index, axis=0)

    @staticmethod
    def append_successive(ts_before, ts_after):
        """
        Append points from ts_after to ts_before.

        Assumed:
          - ts_before is before ts_after
          - only one timestamp may be common:
            last timestamp from ts_before equals the first timestamp from ts_after.
            In that case: the point from ts_after is replacing the one
            from ts_before: this avoids duplicate points.

        :param ts_before: the TS with points before ts_after
        :type ts_before: numpy.array
        :param ts_after: the TS appended to ts_before
        :type ts_after: numpy.array
        :return: ts_before with new points from ts_after
        :rtype: numpy.array
        """
        if len(ts_before) > 0 and len(ts_after) > 0 and ts_before[-1][0] == ts_after[0][0]:
            # avoid duplicate points !!!
            ts_before = np_concatenate((ts_before[0:-1], ts_after), axis=0)
        else:
            ts_before = np_concatenate((ts_before, ts_after), axis=0)
        return ts_before


def pattern_one_unit(value):
    """
    Pattern One is defined on [0,1] interval: see return formula

    :param value:
    :type value:
    :return: -1.0 when ( x < 0.33 or x > 0.66  ) or else 1.0
    :rtype: float
    """
    #
    #  1        ________
    #
    # --------------------------->
    # -1    ____        ____
    #
    #       0               1
    if value < 0.25 or value > 0.75:
        return -1.0
    else:
        return 1.0


def step_pattern(value):
    """
    Example of Pattern defined on [0,1] interval: returns -1 when x < 0; 1 otherwise
    :param value:
    :type value:
    :return: -1.0 when x < 0; 1.0 otherwise
    :rtype: float
    """
    if value < 0.5:
        return -1.0
    else:
        return 1.0


class TsBuilder(object):
    """
    A TsBuilder is helping to build ts data from functions.

    The internal numpy array self._ts is modified by the following
    functions, which can be chained as they return the TsBuilder instance:
      - add_pattern_points()
      - add_several_pattern_occurrences()
      - add_normal_noise()
      - ...

    You can get the ts being built using the getter get_ts.
    It is internally using the TsManagement services on numpy arrays coding for timeseries.
    """

    def __init__(self, ts=None):

        if ts is None:
            self._ts = np_empty([0, 2])
        else:
            self._ts = ts

    def add_points(self, sd, ed, period_step, value_function):
        """
        Add the points
          - with timestamp among np_arange(sd, ed + 1, period_step)
          - with value == value_function(timestamp)
        :param sd: the start of timestamp range
        :type sd: int
        :param ed: the end of timestamp range
        :type ed: int
        :param period_step: the delta between two successive timestamps
        :type period_step: int
        :param value_function: the function evaluating the value.
        :type value_function: function
        :return self is returned to enable chained methods
        :rtype TsBuilder
        """

        timestamps = np_arange(sd, ed + 1, period_step)
        added_ts = np_empty([len(timestamps), 2])

        for t_index, _ in enumerate(timestamps):
            my_timestamp = timestamps[t_index]
            added_ts[t_index][0] = my_timestamp
            added_ts[t_index][1] = value_function(float(my_timestamp))

        self._ts = TsManagement.merge(self._ts, added_ts)

        return self

    def add_pattern_points(self, start_date, end_date, period_step,
                           unit_pattern, translate_value=0.0, scale_value=1.0):
        """
        Insert the pattern with - created or updated- points:
          - with timestamp among np_arange(sd, ed + 1, period_step)
          - with value coded according to the pattern shape

        The pattern shape depends on
          - the original shape on [0, 1] defined by unit_pattern function
          - the translation along the Y-Axis: defined by translate_value
          - the zoom along the Y-axis: defined by scale_value

        :param start_date: the start of timestamp range
        :type start_date: int
        :param end_date: the end of timestamp range
        :type end_date: int
        :param period_step: the delta between two successive timestamps
        :type period_step: int
        :param unit_pattern: the function evaluating the value.
        :type unit_pattern: function
        :param translate_value: translate factor
        :type translate_value: float
        :param scale_value: scale factor
        :type scale_value: float
        :return self is returned to enable chained methods
        :rtype TsBuilder
        """

        period = end_date - start_date

        return self.add_points(
            sd=start_date,
            ed=end_date,
            period_step=period_step,
            value_function=lambda x: unit_pattern((x - start_date) / period) * scale_value + translate_value)

    def add_several_pattern_occurrences(self, period_step, start_dates, durations, unit_pattern_func, y_translations,
                                        y_scales):
        """
        Insert different pattern occurrences: each pattern at position i is composed
        of the following - created or updated- points:
          - with timestamp among np_arange(start_dates[i], durations[i], period_step)
          - with value coded according to the pattern shape at position i

        The pattern shape at i depends on
          - the original shape on [0, 1] defined by unit_pattern function
          - the zoom along the x-axis in order to fit to the durations[i]
          - the translation along the Y-Axis: defined by y_translations[i]
          - the zoom along the Y-axis: defined by y_scales[i]

        :param period_step: the delta between two successive timestamps
        :type period_step: int
        :param start_dates: list of start dates for successive patterns
        :type start_dates: list of int
        :param durations: list of successive durations of the patterns
        :type durations: list of int
        :param unit_pattern_func: function defining the original shape on [0, 1]
        :type unit_pattern_func: function
        :param y_translations: the translation term applied along the Y-axis
        :type y_translations: float
        :param y_scales: the scale factor applied along the Y-axis
        :type y_scales: float
        :return self is returned to enable chained methods
        :rtype TsBuilder
        """

        nb_occurrences = len(start_dates)
        assert ((nb_occurrences == len(durations)) and
                (nb_occurrences == len(y_translations)) and
                (nb_occurrences == len(y_scales)))

        for index in range(nb_occurrences):
            self.add_pattern_points(start_date=start_dates[index],
                                    end_date=start_dates[index] + durations[index],
                                    period_step=period_step,
                                    unit_pattern=unit_pattern_func,
                                    translate_value=y_translations[index],
                                    scale_value=y_scales[index])
        return self

    def add_normal_noise(self, mean, std_dev):
        """
        For each point of TS defined on self: add noise normal distribution.
        :param mean:
        :type mean:
        :param std_dev:
        :type std_dev:
        """
        for point in self._ts:
            point[1] = point[1] + np_random.normal(mean, std_dev)

        return self

    def get_ts(self):
        """
        Gets the Timeseries defined on self.
        """
        return self._ts

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

import json
import string
import time
from datetime import datetime

import numpy as np

from ikats.client.generic_client import GenericClient, is_4xx, is_5xx
from ikats.exceptions import IkatsNotFoundError, IkatsServerError
from ikats.lib import check_type

TEMPLATES = {
    'direct_extract_by_tsuid': '/api/query?start={sd}&end={ed}&tsuid={ts_info}&ms=true',
    'assign_metric': '/api/uid/assign?metric={metric}&tagk={tagk}&tagv={tagv}',
    'add_points': '/api/put?details&ms=true&sync',
    'points_count': '/api/query?start=0&tsuid=sum:1y-count:{tsuid}',
    'get_metric_tags_from_tsuid': '/api/uid/uidmeta?uid={uid}&type={item_type}'
}


class OpenTSDBClient(GenericClient):
    """
    Wrapper for Ikats to connect to OpenTSDB api
    """

    @staticmethod
    def gen_metric_tags(metric=None, tags=None):
        """
        Generate the metric and tags for openTSDB
        OpenTSDB can handle 16M different metrics, 16M tags key (tagk) and 16M tag values (tagv)
        ` Refer to Limited Unique IDs (UIDs) <http://opentsdb.net/docs/build/html/user_guide/writing.html>`_
        Using the date to fill the metric and tags allow to have a useful information in OpenTSDB (import date)

        To not overshoot the limit of 16M, we manage the metric and tags as follow:
        - Metric: hundreds of nanoseconds of the current second (on 7 digits)
                * used to make sure there won't be 2 similar TS imported at the same time
                * example: "0563185" for 56.3185ms, or 56318.5us or 56318500ns
                * value domain: [0; 9999999] < 16M possible values
        - tags matches the following scheme:
            * import_year : year of import (4 digits)
                * example: "2016"
                * value domain: unlimited (but Ikats probably won't be alive in 16M years)
            * import_month_day: month and day on 2 digits each (padding zeros to fill).
                * example: "11_05" for Nov. 05th
                * value domain: 12 months * 31 days = 372 different possibilities < 16M
            * import_time: hours minutes seconds of day on 2 digits each (padding zeros to fill).
                * example: "14_05_35" for 14hours, 5 minutes and 35 seconds"
                * value domain: 24 hours * 60 minutes * 60 seconds = 86400 different possibilities < 16M

        :param metric: metric to use (if known, let None otherwise)
        :param tags: tags to use (if known, let None otherwise)

        :type metric: str or None
        :type tags: dict or None

        :returns: computed metric and tags
        :rtype: tuple
        """

        # Now with nanosecond precision
        now_ns = float("%.9f" % time.time())
        current_date = datetime.fromtimestamp(now_ns)

        # Compute metric and tags based on date
        # metric represents the sub second (in hundreds of ns)
        local_metric = str(int((now_ns - int(now_ns)) * 1E7))
        local_tags = {
            "import_year": current_date.year,
            "import_month_day": current_date.strftime("%m_%d"),
            "import_time": current_date.strftime("%H_%M_%S"),
        }

        local_metric = metric or local_metric
        local_tags = tags or local_tags

        return local_metric, local_tags

    def get_nb_points_of_tsuid(self, tsuid):
        """
        return the effective imported number of points for a given tsuid

        :param tsuid: name of the metric

        :type tsuid: str

        :returns: the imported number of points
        :rtype: int

        :raises IkatsNotFoundError: if no TS with tsuid were found
        :raises SystemError: if openTSDB triggers an error
        """

        response = self.send(root_url=self.session.tsdb_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['points_count'],
                             uri_params={
                                 "tsuid": tsuid
                             })

        results = response.json

        if 'error' in results:
            if 'No such name for' in results['error']['message']:
                raise IkatsNotFoundError("OpenTSDB Error : %s" % (results['error']['message']))
            else:
                raise SystemError("OpenTSDB Error : %s" % (results['error']['message']))

        # Extract Nb points successfully imported
        # by summing the number of points of each year
        return sum([int(x) for x in results[0]['dps']])

    def _get_metric_tags_from_tsuid(self, tsuid):
        """
        Get the metric and tags of a TSUID provided from tsdb-uid table

        :param tsuid: TSUID to get info from
        :type tsuid: str

        :returns: the metric and tags
        :rtype: tuple (metric, tags)

        :raises ValueError: if TSUID is unknown
        :raises ValueError: if OpenTSDB result can't be parsed
        """

        if tsuid:
            # extracting uids by cutting tsuid in slices of 6 characters
            uids = [tsuid[i:i + 6] for i in range(0, len(tsuid), 6)]
        else:
            raise ValueError("TSUID incorrect (got:%s)" % tsuid)

        metric = None
        tags = {}
        tag_key = ''
        for i, uid in enumerate(uids):
            if i == 0:
                item_type = 'metric'
            elif i % 2 == 0:
                item_type = 'tagv'
            else:
                item_type = 'tagk'

            response = self.send(root_url=self.session.tsdb_url,
                                 verb=GenericClient.VERB.GET,
                                 template=TEMPLATES['get_metric_tags_from_tsuid'],
                                 uri_params={
                                     "uid": uid,
                                     "item_type": item_type
                                 })

            if 200 <= response.status_code < 300:
                try:
                    result = response.json['name']
                except KeyError:
                    raise ValueError("OpenTSDB result not parsable (got:%s)" % response.status_code)
            else:
                raise ValueError("UID unknown (got:%s)" % response.status_code)

            if item_type == 'metric':
                metric = result
                tag_key = ''
            elif item_type == 'tagk':
                tag_key = result
                tags[tag_key] = None
            else:
                if tag_key in tags:
                    tags[tag_key] = result

        return metric, tags

    def assign_metric(self, metric, tags):
        """
        From a defined metric and tags, generate the corresponding TSUID

        :param metric:
        :param tags:

        :returns: the TSUID
        :rtype: str

        """

        response = self.send(root_url=self.session.tsdb_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['assign_metric'],
                             uri_params={
                                 "metric": metric,
                                 "tagk": ','.join([str(k) for k, v in tags.items()]),
                                 "tagv": ','.join([str(v) for k, v in tags.items()])
                             })

        results = response.json

        # Initializing tsuid with metric uid retrieved from OpenTSDB json response
        tsuid = self._extract_uid_from_json(item_type='metric', value=metric, data=results)

        # Retrieving and concatenating by pair [ tagk + tagv ] uids from OpenTSDB json response
        tagkv_items = [self._extract_uid_from_json(item_type='tagk', value=str(k), data=results) +
                       self._extract_uid_from_json(item_type='tagv', value=str(v), data=results)
                       for k, v in tags.items()]

        # Concatenating [tagk + tagv] uids to previously initialized tsuid, after having sorted them in
        # increasing order
        tsuid += ''.join(item for item in sorted(tagkv_items))

        return tsuid

    @classmethod
    def _extract_uid_from_json(cls, item_type, value, data):
        """
        Retrieve uid corresponding to type and value parameters parsing json response from OpenTSDB
        (see http://opentsdb.net/docs/build/html/api_http/uid/assign.html Response for json format)

        :param item_type: type of element referenced in OpenTSDB
        :type item_type: str must be one of following ('metric', 'tagk' or 'tagv')

        :param value: value of corresponding type seeking
        :type value: str

        :param data: json response from OpenTSDB to an uid assignment
        :type data: dict

        :returns: uid stored in database
        :rtype: str
        """
        if value in data[item_type]:
            # new uid created
            return data[item_type][value]
        if value in data[item_type + '_errors']:
            uid = str(data[item_type + '_errors'][value]).split(':')[1].strip()
            # Test if returned an hex value
            if all(c in string.hexdigits for c in uid):
                # uid already exist, return value
                return uid
            # impossible to create new id because of bad format provided
            raise ValueError(
                "UID assignment : error when assigning new (item_type=%s) (value=%s) from openTSDB : BAD FORMAT"
                % (item_type, value))
        raise ValueError("not a valid JSON")

    def get_ts_by_tsuid(self, tsuid, sd, ed=None):
        """
        Requests TS data for a specific *tsuid* and corresponding range (defined by *sd* and *ed*)

        Corresponding web app resource operation: **extractByTSUID**

        :param tsuid: name of the metric to extract data from
        :param sd: start date (Timestamp Epoch format in milliseconds)
        :param ed: end date (Timestamp Epoch format in milliseconds) (now if omitted)

        :type tsuid: str
        :type sd: int
        :type ed: int

        :returns: the data associated to the tsuid
            Numpy array is a 2D array where:
                * Column 1 represents the timestamp as numpy.int64 format
                * Column 2 represents the value associated to this timestamp as numpy.float64

        :rtype: numpy array

        :raises TypeError: if *sd* is not a number
        :raises ValueError: if *sd* is negative

        :raises TypeError: if *ed* is not a number
        :raises ValueError: if *ed* is negative

        :raises TypeError: if *ag* is not a str
        """

        # Check inputs
        check_type(value=sd, allowed_types=int, var_name="sd", raise_exception=True)
        if sd < 0:
            self.session.log.error("sd must be positive (got: %s)", sd)
            raise ValueError("sd must be positive (got: %s)" % sd)
        if ed is None:
            ed = int(time.time() * 1000)
            self.session.log.warning("End date missing, 'now' will be used: %s", ed)
        else:
            check_type(value=ed, allowed_types=int, var_name="ed", raise_exception=True)
            if ed < 0:
                self.session.log.error("ed must be positive (got: %s)", ed)
                raise ValueError("ed must be positive (got: %s)" % ed)
            if ed < sd:
                self.session.log.error("ed must be greater than sd (got: %s < %s)", ed, sd)
                raise ValueError("ed must be greater than sd (got: %s < %s)" % (ed, sd))
            if ed == sd:
                # ed should be greater than sd
                ed += 1

        # Filling query parameters
        uri_params = {
            "sd": sd,
            "ed": ed,
            "ts_info": "avg:" + tsuid
        }

        # Number of retry to perform in case of dynamic read/write issues (see below)
        max_retry_count = 1
        retry_count = 0
        while retry_count <= max_retry_count:
            retry_count += 1

            response = self.send(root_url=self.session.tsdb_url,
                                 verb=GenericClient.VERB.GET,
                                 template=TEMPLATES['direct_extract_by_tsuid'],
                                 uri_params=uri_params)

            # Check if at least one entry is returned
            try:

                # Check if data are returned
                # No data may indicate the data are not yet flushed into database by the server (async-hbase)
                # This may occur when data are read shortly after they have been put to database
                if 'dps' not in response.json[0] or not response.json[0]['dps']:
                    # Wait 4 seconds before retrying
                    time.sleep(4)
                    continue

                # Converts to numpy Arrays
                dps = response.json[0]['dps']
                array = np.array([[int(k), float(v)] for k, v in dps.items()], dtype=object)

                # Sort array by date
                # The conversion JSON to python dict was performed automatically
                # Because the python dict is not ordered by key, the sort operation is mandatory
                array = array[array[:, 0].argsort()]
            except IndexError:
                array = np.array([])
            except KeyError:
                raise ValueError(response.json)
            return array
        raise IkatsServerError("Backend didn't provide the points")

    def add_points(self, tsuid, data):
        """

        :param tsuid: TSUID to use
        :param data: points as array

        :type tsuid: str
        :type data: list

        :returns: the start_date, end_date, nb_points
        """

        metric, tags = self._get_metric_tags_from_tsuid(tsuid=tsuid)

        # Building body with points
        json_data = []
        for point in data:
            json_data.append({
                "metric": metric,
                "timestamp": str(point[0]).zfill(13),
                "value": point[1],
                "tags": tags
            })

        response = self.send(root_url=self.session.tsdb_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['add_points'],
                             data=json.dumps(json_data))

        if "success" in response.data and response.data["success"] != len(data):
            self.session.log.debug(response.data)
            raise IkatsServerError("Database wrote only %s points out of %s %s" % (response.data["success"], len(data),
                                                                                   response.data))

        is_4xx(response, "Unexpected client error: {code}")
        is_5xx(response, "Unexpected server error: {code}")

        return data[0][0], data[-1][0], len(data)

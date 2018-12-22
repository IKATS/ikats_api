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

import time
from datetime import datetime
import random
import re
import string
import requests

from ikats.client import RestClient
from ikats.exception import IkatsConflictError

TEMPLATES = {
    # TSDB
    'direct_extract_by_tsuid': '/api/query?start={sd}&end={ed}&tsuid={ts_info}&ms=true'
}


class OpenTSDB(RestClient):
    """
    Wrapper for Ikats to connect to OpenTSDB client
    """

    NON_INHERITABLE_PATTERN = re.compile("^qual(.)*|ikats(.)*|funcId")

    def create_tsuid(self, fid, show_details=False):
        """
        Create a reference of timeseries in openTSDB without creating any data

        :param fid: Functional Identifier of the TS in Ikats
        :param show_details: Show metric and tags if set to True

        :type fid: str
        :type show_details: bool

        :return: the timeseries reference in database (tsuid)
        :rtype: str

        :raises IkatsConflictError: if TSUID already exist
        """
        tdm = TemporalDataMgr(ikats_session=self.session)
        try:
            # check if fid already associated to an existing tsuid
            tsuid = tdm.get_tsuid_from_func_id(func_id=fid)
            # if fid already exist in database, raise a conflict exception
            raise IkatsConflictError("%s already associated to an existing tsuid: %s" % (fid, tsuid))

        except ValueError:
            # here creation of a new tsuid
            metric, tags = self._gen_metric_tags()

            # formatting request
            url = "http://%s:%s/api/uid/assign?metric=%s&tagk=%s&tagv=%s" \
                  % (config_reader.get('cluster', 'opentsdb.read.ip'),
                     int(config_reader.get('cluster', 'opentsdb.read.port')),
                     metric,
                     ','.join([str(k) for k, v in tags.items()]),
                     ','.join([str(v) for k, v in tags.items()]))

            results = requests.get(url=url).json()

            # initializing tsuid with metric uid retrieved from opentsdb json response
            tsuid = self._extract_uid_from_json(item_type='metric', value=metric, json=results)

            # retrieving and concatenating by pair [ tagk + tagv ] uids from opentsdb json response
            tagkv_items = [self._extract_uid_from_json(item_type='tagk', value=str(k), json=results) +
                           self._extract_uid_from_json(item_type='tagv', value=str(v), json=results)
                           for k, v in tags.items()]

            # concatenating [tagk + tagv] uids to previously initialized tsuid, after having sorted them in
            # increasing order
            tsuid += ''.join(item for item in sorted(tagkv_items))

            # finally importing tsuid/fid pair in non temporal database
            tdm.import_fid(tsuid=tsuid, fid=fid)

            if show_details:
                return tsuid, metric, tags

            return tsuid

    @classmethod
    def inherit_properties(cls, tsuid, parent):
        """
        :param tsuid: TSUID of the TS (which will inherit properties)
        :param parent: TSUID of parent TS (from which inheritance will be done)
        :type tsuid: str
        :type parent: str
        """
        tdm = TemporalDataMgr()
        try:
            metadata = tdm.get_meta_data([parent])[parent]
            for meta_name in metadata:
                if not OpenTSDB.NON_INHERITABLE_PATTERN.match(meta_name):
                    tdm.import_meta_data(tsuid=tsuid, name=meta_name, value=metadata[meta_name], force_update=True)
        except(ValueError, TypeError, SystemError) as exception:
            cls.logger.warning(
                "Can't get metadata of parent TS (%s), nothing will be inherited; \nreason: %s", parent, exception)

    @classmethod
    def import_ts(cls, fid, data, parent=None,
                  qsize=100000,
                  threads_count=1,
                  generate_metadata=True,
                  sparkified=False):
        """
        Import TS data points in database using OpenTSDB Http client

        To use multi threading import, set threads_count >1 and sparkified to False.
        Using sparkified forbid the usage of multi-threading (already handled by spark tasks)

        :param fid: Functional Identifier of the TS in Ikats
        :param data: array of points where first column is timestamp (EPOCH ms) and second is value (float compatible)
        :param parent: optional, default None: TSUID of inheritance parent
        :param generate_metadata: (optional) Indicates if the following metadata shall be generated (True:default)
                                  or not (False):
                                    qual_nb_points
                                    ikats_start_date
                                    ikats_end_date
        :param qsize: (optional) Size of the chunk (100000 is default) each item contains a set of data points to send.
                       Don't set too big value to avoid Out of Memory issues
                       Don't set too low value to avoid contention point on waiting for queue to have a free slot.
        :param threads_count: Number of jobs (sending point) to start with
        :param sparkified: set to True to prevent from having multi-processing
                           and to handle correctly the creation of TS by chunk

        :type fid: str
        :type data: np.array
        :type parent: str
        :type qsize: int or None
        :type threads_count: int or None
        :type sparkified: bool

        :return: an object containing several information about the import
        :rtype: dict

        :raises ValueError: if a Functional Identifier (fid) is not provided
        :raises TypeError: if a Functional Identifier (fid) is not a string
        :raises ValueError: if the functional identifier couldn't be created
        :raises SystemError: if the functional identifier couldn't be created
        """

        # Input checks
        if fid is None or fid == "":
            raise ValueError('Functional id must be filled')

        # Get an instance of Temporal Data Manager
        tdm = TemporalDataMgr()

        if sparkified:
            # Force single thread if sparkified (no parallel job on paralleled tasks)
            threads_count = 1
        # Create connection
        client = HttpClient(qsize=qsize, threads_count=threads_count)

        # Check existing TSUID
        try:
            tsuid = tdm.get_tsuid_from_func_id(fid)
            # Use tsuid to find the metric and tags
            metric, tags = cls._get_metric_tags_from_tsuid(tsuid=tsuid)
        except ValueError:
            # No match, we will compute the tsuid, metric and tags
            cls.logger.info("No information for FID %s in base (will create new TS)", fid)
            tsuid, metric, tags = cls.create_tsuid(fid=fid, show_details=True)

        # Define metric and tags
        metric, tags = cls._gen_metric_tags(metric, tags)

        # Metadata init/calc
        nb_points = len(data)
        if type(data) is list:
            start_date = int(data[0][0])
            end_date = int(data[-1][0])
        else:
            # Assuming it is a np.array
            start_date = int(data[0, 0])
            end_date = int(data[-1, 0])

        # Send the data to the send_queue
        result = client.send_http(metric=metric, tags=tags, data_points=data)

        if not sparkified:
            try:
                # Create Functional identifier
                tdm.import_fid(tsuid=tsuid, fid=fid)
            except (ValueError, TypeError, SystemError) as exception:
                cls.logger.error("Can't create the FID [%s] for TSUID [%s], reason: \n%s", fid, tsuid, exception)
                raise
            except IndexError:
                cls.logger.info("FID [%s] for TSUID [%s] already exist", fid, tsuid)

        cls.logger.info("Import speed: %.3f points/s (%s points)", result.speed(), result.success)

        if result.failed:
            cls.logger.error("Only %.1f%% (%d/%d) of points have been saved to %s", (result.success / nb_points),
                             result.success, nb_points, fid)

        # Backward compatibility, store funcId as metadata
        tdm.import_meta_data(tsuid, 'funcId', fid, data_type=DTYPE.string, force_update=True)

        if generate_metadata:
            metadata = tdm.get_meta_data([tsuid])[tsuid]
            # Create or update the metadata
            if 'ikats_start_date' not in metadata or \
                    'ikats_start_date' in metadata and \
                    start_date < int(metadata['ikats_start_date']):
                tdm.update_meta_data(tsuid, 'ikats_start_date', start_date, data_type=DTYPE.date, force_create=True)

            if 'ikats_end_date' not in metadata or \
                    'ikats_end_date' in metadata and \
                    end_date > int(metadata['ikats_end_date']):
                tdm.update_meta_data(tsuid, 'ikats_end_date', end_date, data_type=DTYPE.date, force_create=True)

            tdm.update_meta_data(tsuid, 'qual_nb_points', result.success, data_type=DTYPE.number, force_create=True)

            # Inherit from parent when it is defined
            if parent is not None:
                cls.inherit_properties(tsuid, parent)

        result = {
            'status_code': True,
            'errors': result.errors,
            'numberOfSuccess': result.success,
            'summary': "%.2f%% points imported" % (100 * result.success / len(data)),
            'tsuid': tsuid,
            'funcId': fid,
            'responseStatus': 200
        }

        return result

    @classmethod
    def _get_tsuid_from_metric_tags(cls, metric, ed=None, timeout=120, **tags):
        """
        return the TSUID  (and effective imported number of points) from a metric name and a list of tags

        :param metric: name of the metric
        :param ed: end date of the ts to get (EPOCH ms)
        :param tags: dict of tags key and tags values

        :type metric: str
        :type ed: int
        :type tags: dict

        :return: the TSUID and the imported number of points
        :rtype: tuple

        :raises ValueError: if more than 1 TS matching the criteria exists in openTSDB
        :raises ValueError: if no TS with metric and tags were found
        :raises SystemError: if openTSDB triggers an error
        """

        # get Ikats information
        config_reader = ConfigReader()

        # Build a string like "{tagk:tagv,tagk2:tagv2,tagk3:tagv3}"
        tag_string = "{%s}" % ','.join(["%s=%s" % (k, v) for k, v in tags.items()])

        # Send the request to get the TSUID information

        q_ed = ""
        if ed is not None:
            q_ed = "end=%s&" % int(ed)

        # The "random" query parameter is a trick to not hit opentsdb cache
        url = "http://%s:%s/api/query?start=0&%sshow_tsuids=true&ms=true&m=sum:%sms-count:%s%s&_=%s" % (
            config_reader.get('cluster', 'opentsdb.read.ip'),
            int(config_reader.get('cluster', 'opentsdb.read.port')),
            q_ed,
            int(ed + 1),
            metric, tag_string,
            random.random())

        results = requests.get(
            url=url,
            timeout=timeout
        ).json()
        if 'error' in results:
            if 'No such name for' in results['error']['message']:
                raise ValueError("OpenTSDB Error : %s (url: %s)" % (results['error']['message'], url))
            else:
                raise SystemError("OpenTSDB Error : %s (url: %s)" % (results['error']['message'], url))

        # Extract TSUID
        try:
            tsuid = results[0]['tsuids'][0]
        except IndexError:
            cls.logger.error("No results for url : %s", url)
            raise ValueError("No results for url : %s" % url)

        # Extract Nb points successfully imported
        # The for loop may have at most 2 loops because of the way openTSDB output is built
        nb_points = 0
        for point in results[0]['dps']:
            nb_points += int(results[0]['dps'][point])

        # Do some checks
        if len(results[0]['tsuids']) > 1:
            cls.logger.error("Too many results: %s", len(results['results']))
            raise ValueError("Too many results: %s" % len(results['results']))

        return tsuid, nb_points

    @classmethod
    def get_nb_points_from_tsuid(cls, tsuid, ed=None, timeout=300):
        """
        return the effective imported number of points for a given tsuid

        :param tsuid: name of the metric
        :param ed: end date of the ts to get (EPOCH ms)
        :param timeout: timeout for the request (in seconds)

        :type tsuid: str
        :type ed: int
        :type timeout: int

        :return: the imported number of points
        :rtype: int

        :raises ValueError: if no TS with tsuid were found
        :raises SystemError: if openTSDB triggers an error
        """

        # Get Ikats information
        config_reader = ConfigReader()

        # Send the request to get the TSUID information
        if ed is None:
            # Retrieve end date from metadata
            tdm = TemporalDataMgr()
            metadata = tdm.get_meta_data([tsuid])[tsuid]
            # Create or update the metadata
            ed = int(metadata['ikats_end_date'])

        q_ed = "end=%s&" % int(ed)

        url = "http://%s:%s/api/query?start=0&%s&ms=true&tsuid=sum:%sms-count:%s" % (
            config_reader.get('cluster', 'opentsdb.read.ip'),
            int(config_reader.get('cluster', 'opentsdb.read.port')),
            q_ed,
            int(ed + 1),
            tsuid)
        results = requests.get(
            url=url,
            timeout=timeout
        ).json()
        if 'error' in results:
            if 'No such name for' in results['error']['message']:
                raise ValueError("OpenTSDB Error : %s (url: %s)" % (results['error']['message'], url))
            else:
                raise SystemError("OpenTSDB Error : %s (url: %s)" % (results['error']['message'], url))

        # Extract Nb points successfully imported
        # The for loop may have at most 2 loops because of the way openTSDB output is built
        nb_points = 0
        for point in results[0]['dps']:
            nb_points += int(results[0]['dps'][point])

        return nb_points

    @classmethod
    def _get_metric_tags_from_tsuid(cls, tsuid):
        """
        Get the metric and tags of a TSUID provided from tsdb-uid table

        :param tsuid: TSUID to get info from
        :type tsuid: str

        :return: the metric and tags
        :rtype: tuple (metric, tags)

        :raises ValueError: if TSUID is unknown
        :raises ValueError: if OpenTSDB result can't be parsed
        """

        # get Ikats information
        config_reader = ConfigReader()

        if tsuid:
            # extracting uids by cutting tsuid in slices of 6 characters
            uids = [tsuid[i:i + 6] for i in range(0, len(tsuid), 6)]
        else:
            raise ValueError("TSUID incorrect (got:%s)" % tsuid)

        metric = None
        tags = {}
        for i, uid in enumerate(uids):
            if i == 0:
                item_type = 'metric'
            elif i % 2 == 0:
                item_type = 'tagv'
            else:
                item_type = 'tagk'

            url = "http://%s:%s/api/uid/uidmeta?uid=%s&type=%s" % (
                config_reader.get('cluster', 'opentsdb.read.ip'),
                int(config_reader.get('cluster', 'opentsdb.read.port')),
                uid,
                item_type)

            results = requests.get(url=url)
            if 200 <= results.status_code < 300:
                try:
                    result = results.json()['name']
                except:
                    raise ValueError("OpenTSDB result not parsable (got:%s)" % results.status_code)
            else:
                raise ValueError("UID unknown (got:%s)" % results.status_code)

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

    @classmethod
    def _gen_metric_tags(cls, metric=None, tags=None):
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

        :return: computed metric and tags
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

        cls.logger.debug("Using metric=%s and tags=%s", local_metric, str(local_tags))

        return local_metric, local_tags

    @classmethod
    def _extract_uid_from_json(cls, item_type, value, json):
        """
        Retrieve uid corresponding to type and value parameters parsing json response from opentsdb
        (see http://opentsdb.net/docs/build/html/api_http/uid/assign.html Response for json format)

        :param item_type: type of element referenced in opentsdb
        :type item_type: str must be one of following ('metric', 'tagk' or 'tagv')

        :param value: value of corresponding type seeking
        :type value: str

        :param json: json response from opentsdb to an uid assignment
        :type json: dict

        :return: uid stored in database
        :rtype: str
        """
        if value in json[item_type]:
            # new uid created
            return json[item_type][value]
        elif value in json[item_type + '_errors']:
            uid = str(json[item_type + '_errors'][value]).split(':')[1].strip()
            # Test if returned an hex value
            if all(c in string.hexdigits for c in uid):
                # uid already exist, return value
                return uid
            else:
                # impossible to create new id because of bad format provided
                raise ValueError(
                    "UID assignment : error when assigning new (item_type=%s) (value=%s) from openTSDB : BAD FORMAT"
                    % (item_type, value))

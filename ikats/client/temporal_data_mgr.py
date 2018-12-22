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

import os.path
import uuid
from time import time
from enum import Enum

import numpy as np

from ikats.client import RestClient
from ikats.exception import IkatsNotFoundError, IkatsConflictError, IkatsException, IkatsInputError, \
    IkatsServerException
from ikats.utils import check_type

# List of templates used to build URL.
#
# * Key corresponds to the web app method to use
# * Value contains
#    * the pattern of the url to connect to
TEMPLATES = {
    'remove_ts': '/ts/{tsuid}',
    'extract_by_metric': '/ts/extract/metric/{metric}',
    'get_ts_list': '/ts/tsuid',
    'get_ts_meta': '/ts/tsuid/{tsuid}',
    'extract_by_tsuid': '/ts/extract/tsuid',
    'import_data': '/ts/put/{metric}',
    'get_fid': '/metadata/funcId/{tsuid}',
    'import_fid': '/metadata/funcId/{tsuid}/{fid}',
    'delete_fid': '/metadata/funcId/{tsuid}',
    'lookup_meta_data': '/metadata/list/json',
    'import_meta_data': '/metadata/import/{tsuid}/{name}/{value}',
    'update_meta_data': '/metadata/{tsuid}/{name}/{value}',
    'import_meta_data_file': '/metadata/import/file',
    'dataset_create': '/dataset/import/{data_set}',
    'dataset_read': '/dataset/{name}',
    'get_data_set_list': '/dataset',
    'dataset_remove': '/dataset/{name}',
    'dataset_deep_remove': '/dataset/{name}?deep=true',
    'search': '/ts/lookup/{metric}',
    'ts_match': '/metadata/tsmatch',
    'get_one_functional_identifier': '/metadata/funcId/{tsuid}',
    'search_functional_identifier_list': '/metadata/funcId'
}


class DTYPE(Enum):
    """
    Enum used for Data types of Meta data
    """
    string = "string"
    date = "date"
    number = "number"
    complex = "complex"


class TemporalDataMgr(RestClient):
    """
    Temporal Data Manager client used to connect to JAVA Ikats API
    """

    def __init__(self, *args, **kwargs):
        super(TemporalDataMgr, self).__init__(*args, **kwargs)

    def import_ts_data(self, metric, data, fid, tags=None):
        """
        Import a data into TemporalDataManager

        Corresponding web app resource operation: **import**

        if *data* argument is a **numpy array**, the content shall be formatted as follow
            | [
            |     [time1,value1],
            |     [time2,value2],
            |     [time3,value3],
            |     ...
            | ]
        where *timeX* are date in **numpy.int64** format

        .. warning::
           Multi values (several values per timestamp) are not handled by this method

        if *data* argument is a **file**, the file shall be .csv formatted as follow:

           | timestamp;value
           | 2015-01-01T01:23:45.000+0100;3
           | 2015-01-01T01:23:46.000+0100;12564
           | ...

        :param metric: metric name
        :param data: path of the file containing data or Numpy Array
        :param tags: list of tags to be applied to the full metric
        :param fid : functional identifier. required


        :type metric: str
        :type data: str (file path) or numpy array
        :type tags: dict or None
        :type fid: str

        :return: Execution status_code
        :rtype: dict containing information :

           | {
           |     'status_code' : True if import successful, False otherwise,
           |     'errors' : *dict of errors*,
           |     'numberOfSuccess': *Number of successful imported entries*,
           |     'summary': *summary of import status_code*,
           |    'tsuid': *tsuid if created*
           | }

        :raises TypeError: if data is not a numpy array
        :raises FileNotFoundError: if data is not a valid file
        :raises TypeError: if tags is not a dict
        :raises ValueError : if fid is None
        """

        if fid is None:
            raise ValueError('Functional id must not be None')

        if type(data) is np.ndarray:
            # Build a CSV based on array content and open it

            # Generate a temporary and unique filename
            filename = '/tmp/%s.csv' % str(uuid.uuid4())

            self.session.log.debug("Creating file: %s", filename)
            with open(filename, 'w') as opened_file:
                # Write headers
                opened_file.write("timestamp;value\n")
                # Write content
                lines = "".join("%s;%s\n" % (str(np.datetime64(int(data[index][0]), 'ms')), data[index][1])
                                for index in range(len(data)))
                opened_file.write(lines)

        elif type(data) is str:
            if not os.path.isfile(data):
                self.session.log.error("The file [%s] doesn't exists", data)
                raise FileNotFoundError("The file [%s] doesn't exists" % data)
            # The data is already a file formatted to correct format
            filename = data
        else:
            self.session.log.error("'data' must be a valid file path or a numpy array (got: %s %s)", type(data), data)
            raise TypeError("'data' must be a valid file path or a numpy array (got: %s %s)" % (type(data), data))

        if tags is None:
            tags = {}

        if type(tags) is not dict:
            self.session.log.error("tags must be a dict")
            raise TypeError("tags must be a dict")

        # add the funcId to the form fields
        tags['funcId'] = fid
        # List of items to be replaced by in the template
        uri_params = {
            'metric': metric,
        }

        result = {'status_code': False}

        # Different templates to use depending on the presence of data_set
        template = 'import_data'

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.POST,
                              template=TEMPLATES[template],
                              uri_params=uri_params,
                              data=tags,
                              files=filename)

        # In case the data was an array, remove the temporary file previously created
        if type(data) is np.ndarray:
            os.remove(filename)

        if response.status == 200:
            result['status_code'] = True

        result['errors'] = response.json['errors']
        result['numberOfSuccess'] = response.json['numberOfSuccess']
        result['summary'] = response.json['summary']
        result['tsuid'] = response.json['tsuid']
        result['funcId'] = response.json['funcId']
        result['responseStatus'] = response.status

        return result

    def get_ts_by_metric(self, metric, sd, ed=None, tags=None, ag='avg', dp=None, ds=None, di=False):
        """
        Requests data for a specific metric

        Corresponding web app resource operation: **extractByMetric**

        :returns: TS information
        :rtype:
            Numpy array is a 2D array where
                * Column 1 represents the timestamp TODO date format
                * Column 2 represents the value associated to this timestamp
            (The following concerns only the case whe 'di' argument is set)
                * Column 3 represents the standard deviation value of aggregation
                * Column 4 represents the min value of aggregation
                * Column 5 represents the max value of aggregation

        :param metric: name of the metric to extract data from
        :param sd: start date (Timestamp Epoch format in milliseconds)
        :param ed: end date (Timestamp Epoch format in milliseconds).
        :param tags: dict of tags to filter the request
        :param ag: aggregation method
        :param dp: down sampling aggregation period (example: '12s' to obtain a period equal to 12 seconds)
        :param ds: down sampling aggregation method
        :param di: True to return min, max and standard deviation (when both dp and ds are applied)

        :type sd: int
        :type ed: int
        :type tags: dict
        :type ag: str
        :type dp: str
        :type ds: str
        :type di: bool

        :raises TypeError: if *sd* is not a number
        :raises ValueError: if *sd* is negative

        :raises TypeError: if *ed* is not a number
        :raises ValueError: if *ed* is negative

        :raises TypeError: if *tags* is not a dict

        :raises TypeError: if *ag* is not a str
        :raises TypeError: if *ds* is not a str
        :raises TypeError: if *dp* is not a str

        :raises TypeError: if *di* is not a bool

        :raises ValueError: if *di*=True and *dp* and *ds* are not set
        """

        # Check inputs
        if type(sd) != int:
            self.session.log.error("sd must be a number (got [%s])", sd)
            raise TypeError("sd must be a number (got [%s])" % sd)
        if sd < 0:
            self.session.log.error("sd must be positive")
            raise ValueError("sd must be positive")
        if ed is None:
            ed = int(time() * 1000)
            self.session.log.warning("End date missing, 'now' will be used: %s", ed)
        else:
            if type(ed) != int:
                self.session.log.error("ed must be a number")
                raise TypeError("ed must be a number")
            if ed < 0:
                self.session.log.error("ed must be positive")
                raise ValueError("ed must be positive")
        if type(tags) != dict and tags is not None:
            self.session.log.error("tags must be a dict")
            raise TypeError("tags must be a dict")
        if type(ag) != str:
            self.session.log.error("ag must be a string")
            raise TypeError("ag must be a string")
        if type(di) != bool:
            self.session.log.error("di must be a bool")
            raise TypeError("di must be a bool")
        if di and (ds is None or dp is None):
            self.session.log.error("using di implies ds and dp to be filled")
            raise ValueError("using di implies ds and dp to be filled")
        if type(ds) != str and ds is not None:
            self.session.log.error("ds must be a string (%s)", type(ds))
            raise TypeError("ds must be a string (%s)" % type(ds))
        if type(dp) != str and dp is not None:
            self.session.log.error("dp must be a string")
            raise TypeError("dp must be a string")

        # List of items to be replaced by in the template
        uri_params = {
            'metric': metric,
        }

        # Filling query parameters
        q_params = {'sd': sd, 'ed': ed, 'ag': ag}
        if di:
            q_params['di'] = di
        if ds:
            q_params['ds'] = ds
        if dp:
            q_params['dp'] = dp
        if tags:
            # In url, tags format is : http://root_url?t={a=2,3=4}
            # Here is applied the conversion
            q_params['t'] = str(tags).replace(": ", "=").replace("'", "").replace(" ", "")

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['extract_by_metric'],
                              uri_params=uri_params,
                              q_params=q_params)

        # Check if at least one entry is returned
        if len(response.json) > 0 and 'dps' in response.json[0]:

            # Converts to numpy Arrays
            dps = response.json[0]['dps']
            array = np.array([[np.int64(int(k)), np.float64(float(v))] for k, v in dps.items()])

            if di:
                # Append standard deviation column
                tmp_list = np.array(list(response.json[1]['dps'].values()))
                array = np.c_[array, tmp_list]

                # Append min column
                tmp_list = np.array(list(response.json[2]['dps'].values()))
                array = np.c_[array, tmp_list]

                # Append max column
                tmp_list = np.array(list(response.json[3]['dps'].values()))
                array = np.c_[array, tmp_list]

            # Sort array by date
            # The conversion JSON to python dict was performed automatically
            # Because the python dict is not ordered by key, the sort operation is mandatory
            array = array[array[:, 0].argsort()]
        else:
            array = np.array([])
        return array

    def get_ts(self, tsuid_list, sd=None, ed=None):
        """
        Retrieve the data corresponding to a ts (or a list of ts) without knowing date range

        Corresponding web app resource operation: **lookupMetaData** and **extractByTSUID**

        .. note::
            if omitted, *sd* (start date) and *ed* (end date) will be retrieved from meta data for each TS
            if you want a fixed windowed range, set *sd* and *ed* manually (but be aware that the TS may be
            not completely gathered)

        .. note::
            If no range is provided and no meta data are found, this method will compute the 3 elementary statistics:
               * ikats_start_date : First date of the TS
               * ikats_end_date : Last date of the TS
               * qual_nb_points : Number of points of the TS

        :param tsuid_list:
        :param sd: optional starting date (timestamp in ms from epoch)
        :param ed: optional ending date (timestamp in ms from epoch)

        :type tsuid_list: str or list
        :type sd: int or None
        :type ed: int or None

        :returns: a list of ts data as numpy array
        :rtype: list of numpy array

        :raises TypeError: if *tsuid_list* is neither a list nor a string
        """

        if type(tsuid_list) is str:
            tsuid_list = [tsuid_list]
        if type(tsuid_list) is not list:
            self.session.log.error("get_ts: tsuid_list must be a list or str")
            raise TypeError("tsuid_list must be a list or str")

        # Result list
        result = []

        metadata = None

        if sd is None:
            # Start date is missing, looking in meta data
            metadata = self.get_meta_data(tsuid_list)

        for tsuid in tsuid_list:

            # used_sd and used_ed are the real values that are used for time range
            used_sd = sd
            used_ed = ed

            # Flag allowing the calculation of dates (if date not found in meta data)
            calc_dates = False
            if sd is None:
                if tsuid not in metadata:
                    metadata[tsuid] = {}
                if 'ikats_start_date' in metadata[tsuid]:
                    used_sd = int(metadata[tsuid]['ikats_start_date'])
                else:
                    self.session.log.warning("no 'ikats_start_date' meta data for ts %s", tsuid)
                    # Date not found, preparing for dates calculation
                    calc_dates = True
                    # Set the start date to the minimum allowed date (1 = 1970-01-01T00:00:00Z)
                    used_sd = 1
            if ed is None:
                if metadata is None:
                    # Get the meta data only if not got before
                    metadata = self.get_meta_data(tsuid_list)
                if tsuid not in metadata:
                    metadata[tsuid] = {}
                if 'ikats_end_date' in metadata[tsuid]:
                    used_ed = int(metadata[tsuid]['ikats_end_date'])
                else:
                    # Date not found, preparing for dates calculation
                    # No need to manage else case because None will be interpreted as 'now'
                    calc_dates = True

            # Get data and append to result
            result.append(self.get_ts_by_tsuid(tsuid, used_sd, used_ed))

            # Calculate the start date, end date and number of points of the TS
            if calc_dates:
                # Last TS gathered is stored in results[-1]
                # The start date is located at first index (results[-1][0])
                # The end date is located at last index (results[-1][-1])
                # and the timestamp column is the first (results[-1][?][0])
                ikats_start_date = int(result[-1][0][0])
                ikats_end_date = int(result[-1][-1][0])
                qual_nb_points = len(result[-1])

                self.session.log.info("Calculating date for %s during get_ts method", tsuid)
                self.session.log.info("   'ikats_start_date' = %s", ikats_start_date)
                self.session.log.info("   'ikats_end_date'   = %s", ikats_end_date)
                self.session.log.info("   'qual_nb_points'   = %s", qual_nb_points)

                self.import_meta_data(tsuid=tsuid, name='ikats_start_date', value=ikats_start_date,
                                      data_type=DTYPE.date)
                self.import_meta_data(tsuid=tsuid, name='ikats_end_date', value=ikats_end_date,
                                      data_type=DTYPE.date)
                self.import_meta_data(tsuid=tsuid, name='qual_nb_points', value=qual_nb_points,
                                      data_type=DTYPE.number)

        return result

    def get_ts_list(self):
        """
        Get the list of all TSUID in database

        :return: the list of TSUID with their associated metrics
        :rtype: list
        """
        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['get_ts_list'])

        return response.json

    def get_ts_meta(self, tsuid):
        """
        Get the info about the TSUID in database

        :param tsuid: name of the metric to extract data from
        :return: the metric, the tags and the funcId of the TSUID
        :rtype: json :
           {"tsuid":"0000110000030003F20000040003F1",
            "funcId":"A320001_1_WS1",
            "metric":"WS1",
            "tags":{"flightIdentifier":"1",
                    "aircraftIdentifier":"A320001"}
           }
        """

        uri_params = {
            'tsuid': tsuid,
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['get_ts_meta'],
                              uri_params=uri_params)

        return response.json

    def get_ts_by_tsuid(self, tsuid, sd, ed=None, ag='avg', old_format=False):
        """
        Requests TS data for a specific *tsuid* and corresponding range (defined by *sd* and *ed*)

        Corresponding web app resource operation: **extractByTSUID**

        :param tsuid: name of the metric to extract data from
        :param sd: start date (Timestamp Epoch format in milliseconds)
        :param ed: end date (Timestamp Epoch format in milliseconds) (now if omitted)
        :param ag: aggregation method (for TS with multiple values, retrieve one point corresponding to aggregate)
        :param old_format: use numpy.datetime64 for timestamp type

        .. see also: openTSDB API from aggregation methods

        .. deprecated::
           old_format flag will be removed in future releases,
           please use numpy.int64 as standard time type

        :type tsuid: str
        :type sd: int
        :type ed: int
        :type ag: str
        :type old_format: bool

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
        if type(sd) != int:
            self.session.log.error("sd must be a number (got: %s)", sd)
            raise TypeError("sd must be a number (got: %s)" % sd)
        if sd < 0:
            self.session.log.error("sd must be positive (got: %s)", sd)
            raise ValueError("sd must be positive (got: %s)" % sd)
        if ed is None:
            ed = int(time() * 1000)
            self.session.log.warning("End date missing, 'now' will be used: %s", ed)
        else:
            if type(ed) != int:
                self.session.log.error("ed must be a number (got: %s)", ed)
                raise TypeError("ed must be a number (got: %s)" % ed)
            if ed < 0:
                self.session.log.error("ed must be positive (got: %s)", ed)
                raise ValueError("ed must be positive (got: %s)" % ed)
            if ed < sd:
                self.session.log.error("ed must be greater than sd (got: %s < %s)", ed, sd)
                raise ValueError("ed must be greater than sd (got: %s < %s)" % (ed, sd))
            if ed == sd:
                # ed should be greater than sd
                ed += 1
        if type(ag) != str:
            self.session.log.error("ag must be a string (got: %s)", ag)
            raise TypeError("ag must be a string (got: %s)" % ag)

        ts_info = ag + ":" + tsuid

        # Filling query parameters
        uri_params = {
            "sd": sd,
            "ed": ed,
            "ts_info": ts_info
        }

        response = self._send(root_url=self.session.tsdb_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['direct_extract_by_tsuid'],
                              uri_params=uri_params)

        # Check if at least one entry is returned
        try:
            # Converts to numpy Arrays
            dps = response.json[0]['dps']
            if old_format:
                array = np.array([[np.datetime64(int(k), 'ms'), np.float64(float(v))] for k, v in dps.items()])
            else:
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

    def get_ts_info(self, metric, query_params=None):
        """
        Returns information about a metric provided in arguments
        Search for metric name in Time series and return information (like tsuid, see below) about the TS

        Corresponding web app resource operation: **search**

        Example:
            ``client.get_ts_info("WS6",{'flightIdentifier':['625','629']})``
        will return:
            | [
            |     {
            |         'metric': 'WS6',
            |         'tags': {'aircraftIdentifier': 'A320001', 'flightIdentifier': '625'},
            |         'tsuid': '0000160000030006630000040003F1'
            |     },
            |     {
            |         'metric': 'WS6',
            |         'tags': {'aircraftIdentifier': 'A320001', 'flightIdentifier': '629'},
            |         'tsuid': '0000160000030006670000040003F1'
            |     }
            | ]

        :param metric: metric name (or ``*`` for all metrics)
        :param query_params: dict to provide the query parameters (free field)

        :type metric: str
        :type query_params: dict

        :returns: the data associated to the *metric*
        :rtype: list of dict

        :raises TypeError: if *query_parameters* is not a dict
        :raises TypeError: if *metric* is not a str
        """

        if type(query_params) is not dict and query_params is not None:
            self.session.log.error('query_params shall be a dict')
            raise TypeError('query_params shall be a dict')

        if type(metric) is not str:
            self.session.log.error('metric shall be a str')
            raise TypeError('metric shall be a str')

        # List of items to be replaced by in the template
        uri_params = {
            'metric': metric,
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['search'],
                              uri_params=uri_params,
                              q_params=query_params)

        results = []

        if response.status == 200:
            # No errors, we can parse
            try:
                if response.json['totalResults'] > 0:
                    results = response.json['results']
            except KeyError:
                assert False, "HTTP response can't be parsed (expecting json containing 'totalResults' key). Got: %s" \
                              % response.text

        return results

    def dataset_create(self, name, description, ts):
        """
        Create a new data set composed of the *tsuid_list*

        Corresponding web app resource operation: **importDataSet**

        :param name: name of the data set
        :param description: short functional description about the content
        :param ts: list of tsuid composing the data set

        :type name: str
        :type description: str
        :type ts: list OR str

        :return: execution status_code (True if success, False otherwise)
        :rtype: bool

        :raises TypeError: if *tsuid_list* is not a list
        """

        if type(ts) is not list:
            self.session.log.error('ts shall be a list')
            raise TypeError('ts shall be a list')

        # List of items to be replaced by in the template
        uri_params = {
            'data_set': name,
        }

        data = {
            'name': name,
            'description': description,
            'tsuidList': ','.join(ts),
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.POST,
                              template=TEMPLATES['dataset_create'],
                              uri_params=uri_params,
                              data=data)

        if response.status_code == 409:
            raise IkatsConflictError("Dataset %s already exist in database" % name)

    def dataset_read(self, name):
        """
        Retrieve the details of a Dataset provided in arguments

        Corresponding web app resource operation: **getDataSet**

        :param name: name of the data set to request TS list from
        :type name: str

        :return:
           information about ts_list and description
           * *ts_list* is the list of TS matching the data_set
           * *description* is the description sentence of the data set
        :rtype: dict

        :raises TypeError: if data_set is not a str
        """

        # Checks inputs
        if type(name) is not str:
            self.session.log.error("name must be a string (got %s)", type(name))
            raise TypeError("name must be a string (got %s)" % type(name))

        # List of items to be replaced by in the template
        uri_params = {
            'name': name
        }

        ret = {
            'ts_list': [],
            'description': None
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['dataset_read'],
                              uri_params=uri_params)

        if response.status_code == 404:
            raise IkatsNotFoundError("Dataset %s not found in database" % name)

        if 'fids' in response.json:
            ret['ts_list'] = response.json['fids']

        if 'description' in response.json:
            ret['description'] = response.json['description']

        return ret

    def dataset_delete(self, name, deep=False):
        """
        Remove data_set from base

        Corresponding web app resource operation: **removeDataSet**

        :param name: name of the data set to delete
        :type name: str

        :param deep: true to deeply remove dataset (TSUID and metadata erased)
        :type deep: boolean

        :return: True if operation is a success, False if error occurred
        :rtype: bool

        .. note::
           Removing an unknown data set results in a successful operation (server constraint)
           The only possible errors may come from server (HTTP status_code code 5xx)

        :raises TypeError: if *name* is not a str
        :raises TypeError: if *deep* is not a bool
        """

        # Checks inputs
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)
        check_type(value=deep, allowed_types=bool, var_name="deep", raise_exception=True)

        # List of items to be replaced by in the template
        uri_params = {
            'name': name
        }

        template = 'dataset_remove'
        if deep:
            template = 'dataset_deep_remove'
        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.DELETE,
                              template=TEMPLATES[template],
                              uri_params=uri_params)

        if response.status_code == 404:
            raise IkatsNotFoundError("Dataset %s not found in database" % name)
        return response

    def dataset_list(self):
        """
        Get the list of all data set and their corresponding description

        :return: key: data set, value: corresponding description : [{'name':name,'description':description}]
        :rtype: list of dict
        """

        response = self._send(
            verb=RestClient.VERB.GET,
            template=TEMPLATES['get_data_set_list'])

        results = []

        try:
            # Keep only the necessary fields from the request
            for content in response.json:
                name = content['name']
                description = content['description']
                results.append({'name': name, 'description': description})
        except IndexError:
            # Return emtpy results if parsing error
            pass
        return results


    def import_fid(self, tsuid, fid):
        """
        Import a functional ID into TemporalDataManager

        :param tsuid: TSUID identifying the TS
        :param fid: Functional identifier

        :type tsuid: str
        :type fid: str

        :raises TypeError: if *tsuid* not a str
        :raises TypeError: if *fid* not a str

        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *fid* is empty
        :raises IndexError: if *fid* exists
        :raises SystemError: if another issue occurs
        """

        # Checks inputs
        if type(tsuid) is not str:
            self.session.log.error("tsuid must be a string (got %s)", type(tsuid))
            raise TypeError("tsuid must be a string (got %s)" % type(tsuid))
        if type(fid) is not str:
            self.session.log.error("fid must be a string (got %s)", type(fid))
            raise TypeError("fid must be a string (got %s)" % type(fid))

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")
        if fid == "":
            self.session.log.error("fid must not be empty")
            raise ValueError("fid must not be empty")

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid,
            'fid': fid
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.POST,
                              template=TEMPLATES['import_fid'],
                              uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status == 200:
            self.session.log.info("TSUID:%s - FID created %s", tsuid, fid)
        elif response.status == 409:
            self.session.log.warning("TSUID:%s - FID already exists (not updated) %s", tsuid, fid)
            raise IndexError("TSUID:%s - FID already exists (not updated) %s" % (tsuid, fid))
        else:
            self.session.log.warning("TSUID:%s - FID %s not created (got %s)", tsuid, fid, response.status)
            raise SystemError("TSUID:%s - FID %s not created (got %s)" % (tsuid, fid, response.status))

    def get_fid(self, tsuid):
        """
        Get a functional ID from its TSUID

        :param tsuid: TSUID identifying the TS

        :type tsuid: str

        :raises TypeError: if *tsuid* not a str
        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *tsuid* doesn't have a FID
        """

        # Checks inputs
        if type(tsuid) is not str:
            self.session.log.error("tsuid must be a string (got %s)", type(tsuid))
            raise TypeError("tsuid must be a string (got %s)" % type(tsuid))

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['get_fid'],
                              uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status == 200:

            if response.json != '{}':
                fid = response.json['funcId']
                self.session.log.debug("TSUID:%s - FID obtained %s", tsuid, fid)
                return fid
            else:
                self.session.log.warning("No FID for TSUID [%s]", tsuid)
                raise IndexError("No FID for TSUID [%s]" % tsuid)
        else:
            raise ValueError("No FID for TSUID [%s]" % tsuid)

    def delete_fid(self, tsuid):
        """
        Delete a functional ID from its TSUID

        :param tsuid: TSUID identifying the TS

        :type tsuid: str

        :raises TypeError: if *tsuid* not a str
        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if FID not deleted
        """

        # Checks inputs
        if type(tsuid) is not str:
            self.session.log.error("tsuid must be a string (got %s)", type(tsuid))
            raise TypeError("tsuid must be a string (got %s)" % type(tsuid))

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.DELETE,
                              template=TEMPLATES['delete_fid'],
                              uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status == 200:
            self.session.log.info("TSUID:%s - FID deleted", tsuid)
        else:
            self.session.log.warning("TSUID [%s] - FID not deleted. Received status_code:%s", tsuid, response.status)
            raise ValueError

    def import_meta_data(self, tsuid, name, value, data_type=DTYPE.string, force_update=False):
        """
        Import a meta data into TemporalDataManager

        Corresponding web app resource operation: **importMetaData**

        :param tsuid: Functional Identifier of the TS
        :param name: Metadata name
        :param value: Value of the metadata
        :param data_type: data type of the meta data
        :param force_update: True to create the meta if not exists (default: False)

        :type tsuid: str
        :type name: str
        :type value: str or number
        :type data_type: DTYPE
        :type force_update: bool

        :return: execution status_code, True if import successful, False otherwise
        :rtype: bool

        :raises TypeError: if *tsuid* not a str
        :raises TypeError: if *name* not a str
        :raises TypeError: if *value* not a str or a number
        :raises TypeError: if *data_type* not a DTYPE
        :raises TypeError: if *force_update* not a bool

        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *name* is empty
        :raises ValueError: if *value* is empty
        """

        # Checks inputs
        if type(tsuid) is not str:
            self.session.log.error("tsuid must be a string (got %s)", type(tsuid))
            raise TypeError("tsuid must be a string (got %s)" % type(tsuid))
        if type(name) is not str:
            self.session.log.error("name must be a string (got %s)", type(name))
            raise TypeError("name must be a string (got %s)" % type(name))
        if type(value) not in [str, int, float]:
            self.session.log.error("value must be a string or a number (got %s)", type(value))
            raise TypeError("value must be a string or a number (got %s)" % type(value))
        if type(data_type) is not DTYPE:
            self.session.log.error("data_type must be a DTYPE (got %s)", type(data_type))
            raise TypeError("data_type must be a DTYPE (got %s)" % type(data_type))

        if type(force_update) is not bool:
            self.session.log.error("force_update must be a bool (got %s)", type(force_update))
            raise TypeError("force_update must be a bool (got %s)" % type(force_update))

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")
        if name == "":
            self.session.log.error("name must not be empty")
            raise ValueError("name must not be empty")
        if value == "":
            self.session.log.error("value must not be empty")
            raise ValueError("value must not be empty")

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid,
            'name': name,
            'value': value
        }

        # Data type of the meta data
        q_params = {'dtype': data_type.value}

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.POST,
                              template=TEMPLATES['import_meta_data'],
                              uri_params=uri_params,
                              q_params=q_params)

        # in case of success, web app returns 2XX
        if response.status == 200:
            self.session.log.info("TSUID:%s - MetaData created %s(%s)=%s", tsuid, name, data_type, value)
            return True
        elif response.status == 409:
            if force_update:
                # Error occurred (can't create a metadata that already exists - conflict)
                # Try to update it because it is wanted
                self.session.log.info("TSUID:%s - MetaData updated %s=%s", tsuid, name, value)
                return self.update_meta_data(tsuid=tsuid, name=name, value=value, force_create=False)
            else:
                self.session.log.warning("TSUID:%s - MetaData already exists (not updated) %s=%s", tsuid, name, value)
                return False
        self.session.log.warning("TSUID [%s] - MetaData not created %s=%s. Received status_code:%s", tsuid, name, value,
                                 response.status)
        return False

    def update_meta_data(self, tsuid, name, value, data_type=DTYPE.string, force_create=False):
        """
        Import a meta data into TemporalDataManager

        Corresponding web app resource operation: **importMetaData**

        :param tsuid: Functional Identifier of the TS
        :param name: Metadata name
        :param value: Value of the metadata
        :param data_type: data type of the meta data (used only if force_create, no type change on existing meta data)
        :param force_create: True to create the meta if not exists (default: False)

        :type tsuid: str
        :type name: str
        :type value: str or number
        :type data_type: DTYPE
        :type force_create: bool

        :return: execution status_code, True if import successful, False otherwise
        :rtype: bool

        :raises TypeError: if *tsuid* not a str
        :raises TypeError: if *name* not a str
        :raises TypeError: if *value* not a str or a number
        :raises TypeError: if *force_create* not a bool

        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *name* is empty
        :raises ValueError: if *value* is empty
        """

        # Checks inputs
        if type(tsuid) is not str:
            self.session.log.error("tsuid must be a string (got %s)", type(tsuid))
            raise TypeError("tsuid must be a string (got %s)" % type(tsuid))
        if type(name) is not str:
            self.session.log.error("name must be a string (got %s)", type(name))
            raise TypeError("name must be a string (got %s)" % type(name))
        if type(value) not in [str, int, float]:
            self.session.log.error("value must be a string or a number (got %s)", type(value))
            raise TypeError("value must be a string or a number (got %s)" % type(value))
        if type(force_create) is not bool:
            self.session.log.error("force_create must be a bool (got %s)", type(force_create))
            raise TypeError("force_create must be a bool (got %s)" % type(force_create))

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")
        if name == "":
            self.session.log.error("name must not be empty")
            raise ValueError("name must not be empty")
        if value == "":
            self.session.log.error("value must not be empty")
            raise ValueError("value must not be empty")

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid,
            'name': name,
            'value': value,
        }

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.PUT,
                              template=TEMPLATES['update_meta_data'],
                              uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status == 200:
            self.session.log.info("TSUID:%s - MetaData updated %s=%s", tsuid, name, value)
            return True
        elif response.status == 404:
            if force_create:
                # Error occurred (can't update a metadata that doesn't exists)
                # Try to create it because it is wanted
                self.session.log.info("TSUID:%s - MetaData created %s=%s", tsuid, name, value)
                return self.import_meta_data(tsuid=tsuid, name=name, value=value, data_type=data_type,
                                             force_update=False)
            else:
                self.session.log.warning("TSUID:%s - MetaData doesn't exists and not Created %s=%s", tsuid, name, value)
                return False
        self.session.log.warning(
            "TSUID [%s] - MetaData not updated %s=%s. Received status_code:%s", tsuid, name, value, response.status)
        return False

    def get_meta_data(self, ts_list):
        """
        Request for metadata of a TS or a list of TS

        Corresponding web app resource operation: **lookupMetaData**

        .. note::
           Accepted format for list of TS are:
               * 'TS1,TS2,TS3,TS4'
               * 'TS1'
               * ['TS1','TS2','TS3','TS4']
               * ['TS1']

        :returns: metadata for each TS
        :rtype: dict (key is TS identifier, value is list of metadata)
            | {
            |     'TS1': {'param1':'value1', 'param2':'value2'},
            |     'TS2': {'param1':'value1', 'param2':'value2'}
            | }

        :param ts_list: list of TS identifier
        :type ts_list: str or list

        :raises TypeError: if *ts_list* is neither a str nor a list
        """

        # Checks inputs
        if type(ts_list) is str:
            # Hack to convert string to list (to homogenize treatment)
            ts_list = ts_list.split(',')
        if type(ts_list) is not list:
            self.session.log.error("ts_list must be a list")
            raise TypeError("ts_list must be a list")

        output_dict = {}

        # It is not possible to have infinite URL length using GET method
        # We have to divide in 'chunks' to not exceed the URL size limit.
        # Commonly, this size is 8KB long (8192 chars)
        # The chunk_size is set to a value which approach this limit with a safety coeff
        chunk_size = 100
        for i in range(0, len(ts_list), chunk_size):
            working_ts_list = ts_list[i:i + chunk_size]

            # Filling query parameters
            q_params = {'tsuid': ','.join(working_ts_list)}

            response = self._send(root_url=self.session.tdm_url,
                                  verb=RestClient.VERB.GET,
                                  template=TEMPLATES['lookup_meta_data'],
                                  q_params=q_params)

            if response.status == 414:
                # The size of the request is too big
                # Decrease the chunk_size above
                self.session.log.error("The size of the request is too big. Contact administrator")
                assert True

            # Format response
            # Converts from
            #   [
            #       {id:1,tsuid:'TS1',name:'unit',value:'meters'},
            #       {id:2,tsuid:'TS1',name:'FlightPhase',value:'TakeOff'}
            #   ]
            # To
            #   {
            #   'TS1':
            #       {
            #           'unit':'meters',
            #           'FlightPhase':'TakeOff'
            #       }
            #   }

            # init the output with ts list as keys
            for ts in working_ts_list:
                output_dict[ts] = {}

            # Fill in meta data for each ts
            if response.json != '{}':
                for content in response.json:

                    # Init the key if first meet
                    if content['tsuid'] not in output_dict:
                        output_dict[content['tsuid']] = {}

                    output_dict[content['tsuid']][content['name']] = content['value']

        return output_dict

    def get_typed_meta_data(self, ts_list):
        """
        Request for metadata of a TS or a list of TS

        Corresponding web app resource operation: **lookupMetaData**

        .. note::
           Accepted format for list of TS are:
               * 'TS1,TS2,TS3,TS4'
               * 'TS1'
               * ['TS1','TS2','TS3','TS4']
               * ['TS1']

        :returns: metadata for each TS
        :rtype: dict (key is TS identifier, value is list of metadata with its associated data type)
            | {
            |     'TS1': {'param1':{'value':'value1', 'type': 'dtype'}, 'param2':{'value':'value2', 'type': 'dtype'}},
            |     'TS2': {'param1':{'value':'value1', 'type': 'dtype'}, 'param2':{'value':'value2', 'type': 'dtype'}}
            | }

        :param ts_list: list of TS identifier
        :type ts_list: str or list

        :raises TypeError: if *ts_list* is neither a str nor a list
        """

        # Checks inputs
        if type(ts_list) is str:
            # Hack to convert string to list (to homogenize treatment)
            ts_list = ts_list.split(',')
        if type(ts_list) is not list:
            self.session.log.error("ts_list must be a list")
            raise TypeError("ts_list must be a list")

        output_dict = {}

        # It is not possible to have infinite URL length using GET method
        # We have to divide in 'chunks' to not exceed the URL size limit.
        # Commonly, this size is 8KB long (8192 chars)
        # The chunk_size is set to a value which approach this limit with a safety coeff
        chunk_size = 100
        for i in range(0, len(ts_list), chunk_size):
            working_ts_list = ts_list[i:i + chunk_size]

            # Filling query parameters
            q_params = {'tsuid': ','.join(working_ts_list)}

            response = self._send(root_url=self.session.tdm_url,
                                  verb=RestClient.VERB.GET,
                                  template=TEMPLATES['lookup_meta_data'],
                                  q_params=q_params)

            if response.status == 414:
                # The size of the request is too big
                # Decrease the chunk_size above
                self.session.log.error("The size of the request is too big. Contact administrator")
                assert True

            # Format response
            # Converts from
            #   [
            #       {id:1,tsuid:'TS1',name:'unit',value:'meters', dtype:'string'},
            #       {id:2,tsuid:'TS1',name:'FlightPhase',value:'TakeOff', dtype:'string'}
            #   ]
            # To
            #   {
            #   'TS1':
            #       {
            #           'unit':{'value':'meters', 'type':'string'},
            #           'FlightPhase': {'value':'TakeOff', 'type':'string'},
            #       }
            #   }

            # init the output with ts list as keys
            for ts in working_ts_list:
                output_dict[ts] = {}

            # Fill in mete data for each ts
            for content in response.json:

                # Init the key if first meet
                if content['tsuid'] not in output_dict:
                    output_dict[content['tsuid']] = {}

                output_dict[content['tsuid']][content['name']] = {'value': content['value'], 'type': content['dtype']}

        return output_dict

    def get_ts_from_meta_data(self, constraint=None):
        """
        From a meta data constraint provided in parameter, the method get a TS list matching these constraints

        Corresponding web app resource operation: **TSMatch**

        Example of constraint:
            | {
            |     frequency: [1, 2],
            |     flight_phase: 8
            | }
        will find the TS having the following meta data:
            | (frequency == 1 OR frequency == 2)
            | AND
            | flight_phase == 8


        :returns: list of TSUID matching the constraints
        :rtype: dict

        :param constraint: constraint definition
        :type constraint: dict

        :raises TypeError: if *constraint* is not a dict
        """

        # Checks inputs
        if constraint is None:
            constraint = {}
        if type(constraint) is not dict:
            self.session.log.error("constraint must be a dict")
            raise TypeError("constraint must be a dict")

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['ts_match'],
                              q_params=constraint)

        result = response.json

        return result

    def get_func_id_from_tsuid(self, tsuid):
        """
        Retrieve the functional identifier resource associated to the tsuid param.
        The resource returned aggregates original tsuid and retrieved fundId.

        :param tsuid: one tsuid value
        :type tsuid: str
        :return: retrieved functional identifier resource
        :rtype: dict having following keys defined:
          - 'tsuid'
          - and 'funcId'
        :raises exception:
            - TypeError: if tsuid is not a str OR status_code 400 (bad request) OR unexpected http status_code
            - ValueError: mismatched result: http status_code 404:  not found
            - ServerError: http status_code for server errors: 500 <= status_code < 600
        """
        if not isinstance(tsuid, str):
            raise TypeError("tsuid type must be str")

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['get_one_functional_identifier'],
                              uri_params={'tsuid': tsuid},
                              data=None,
                              files=None)

        self.__check_fid_errors(response)

        return response.json['funcId']

    def get_tsuid_from_func_id(self, func_id):
        """
        Retrieve the tsuid associated to the func_id param.
        :param func_id: one func_id value
        :type func_id: str
        :return: retrieved tsuid value
        :rtype: str
        :raises exception:
          - TypeError: if unexpected func_id parameter OR status_code 400 (bad request) OR unexpected http status_code
          - ValueError: mismatched result: http status_code 404:  not found
          - ServerError: http status_code for server errors: 500 <= status_code < 600
        """
        if not isinstance(func_id, str):
            raise TypeError("func_id type must be str")

        # empty result => throws ValueError
        res = self.search_functional_identifiers(criterion_type='funcIds', criteria_list=[func_id])

        assert (isinstance(res, list)), "get_tsuid_from_func_id: failed to retrieve json result as list"
        first = res[0]
        assert (isinstance(first, dict)), "get_tsuid_from_func_id: failed to retrieve first item from result list"
        return first['tsuid']

    def search_functional_identifiers(self, criterion_type, criteria_list):
        """
        Retrieve the list of functional identifier records.
        Each resource record aggregates one tsuid and associated fundId.

        Note: partial match will not raise error, contrary to empty match.

        :param criterion_type: defines criterion applicable to this search
        :type criterion_type: str value accepted by server.
          ex: 'tsuids' or 'funcIds'
        :param criteria_list: non empty list of possible values for the criterion type
        :type criteria_list: list of str
        :return: matching list of functional identifier resources: dict having following keys defined:
            - 'tsuid',
            - and 'funcId'
        :rtype: list of dict
        :raises exception:
          - TypeError: if unexpected arguments OR status_code 400 (bad request) OR unexpected http status_code
          - ValueError: mismatched result: http status_code 404:  not found
          - ServerError: http status_code for server errors: 500 <= status_code < 600
        """
        if not isinstance(criterion_type, str):
            raise TypeError("criterion_type type must be str")
        if not isinstance(criteria_list, list):
            raise TypeError("criteria_list type must be list")

        my_filter = dict()
        my_filter[criterion_type] = criteria_list

        self.session.log.debug("search_functional_identifier_list with prepared Filter: %s", str(my_filter))

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.POST,
                              template=TEMPLATES['search_functional_identifier_list'],
                              uri_params=None,
                              data=my_filter,
                              files=None)

        self.__check_fid_errors(response)

        return response.json

    def __check_fid_errors(self, response):
        """
        Inspect http response and throws error if needed, as specified for functional identifier services
        :param response: http response handled
        :type response: type returned by RestClient::_send()
        :raises exception:
            - TypeError: if status_code 400 (bad request) OR unexpected http status_code
            - ValueError: mismatched result: http status_code 404:  not found
            - ServerError: http status_code for server errors: 500 <= status_code < 600
        """

        if response.status == 200:
            self.session.log.debug('200 (ok) %s produced: %s ', response.url, response.json)
        elif response == 400:
            err_msg = '400 (bad request) %s produced: %s' % (response.url, response.json)
            raise TypeError(err_msg)
        elif response.status == 404:
            err_msg = '404 (not found) %s produced: %s' % (response.url, response.json)
            raise ValueError(err_msg)
        else:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise TypeError(err_msg)

            # Note: server error already thrown by  self._send

    def create_table(self, data):
        """
        Create a table

        :param data: data to store
        :type data: dict

        :return: the name of the created table

        :raises IkatsInputError: for any error present in the inputs
        :raises IkatsException: for any other error during the request
        """

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.POST,
                              template=TEMPLATES['create_table'],
                              json_data=data,
                              files=None)

        if response.status == 400:
            err_msg = 'Bad request while creating Table %s produced: %s' % (response.url, response.json)
            raise IkatsInputError(err_msg)
        elif response.status == 409:
            err_msg = 'Conflict detected while creating Table %s produced: %s' % (response.url, response.json)
            raise IkatsConflictError(err_msg)
        elif response.status >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise IkatsServerException(err_msg)

        return response.json

    def list_tables(self, name=None, strict=True):
        """
        List all tables
        If name is specified, filter by name
        name can contains "*", this character is considered as "any chars" (equivalent to regexp /.*/)

        :param name: name to find
        :param strict: consider name without any wildcards

        :type name: str or None
        :type strict: bool

        :return: the list of tables matching the requirements
        :rtype: list

        :raises IkatsInputError: for any error present in the inputs
        :raises IkatsException: for any other error during the request
        """
        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['list_tables'],
                              uri_params={'name': name, 'strict': strict},
                              data=None,
                              files=None)

        if response.status == 404:
            return response.json
        elif response.status >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise IkatsException(err_msg)
        return response.json

    def read_table(self, name):
        """
        Reads the data blob content: for the unique table identified by id.

        :param name: the name of the raw table to get data from
        :type name: str

        :return: the content data stored.
        :rtype: bytes or str or object

        :raise IkatsNotFoundError: no resource identified by ID
        :raise IkatsException: any other error
        """

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.GET,
                              template=TEMPLATES['read_table'],
                              uri_params={'name': name},
                              data=None,
                              files=None)

        if response.status == 400:
            raise IkatsInputError("Wrong input: [%s]" % name)
        if response.status == 404:
            raise IkatsNotFoundError("Table %s not found" % name)
        elif response.status >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise IkatsException(err_msg)

        return response.json

    def delete_table(self, name):
        """
        Delete a table

        :param name: the name of the table to delete
        :type name: str

        """

        response = self._send(root_url=self.session.tdm_url,
                              verb=RestClient.VERB.DELETE,
                              template=TEMPLATES['delete_table'],
                              uri_params={'name': name},
                              data=None,
                              files=None)

        if response.status == 400:
            raise IkatsInputError("Wrong input: [%s]" % name)
        if response.status == 404:
            raise IkatsNotFoundError("Table %s not found" % name)
        elif response.status >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise IkatsException(err_msg)

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

from enum import Enum

from ikats.client import GenericClient
from ikats.exceptions import *
from ikats.lib import check_type, check_is_fid_valid, check_is_valid_ds_name
from ikats.client.generic_client import check_http_code, is_404

# List of templates used to build URL.
#
# * Key corresponds to the web app method to use
# * Value contains
#    * the pattern of the url to connect to
TEMPLATES = {
    'remove_ts': '/ts/{tsuid}',
    'extract_by_metric': '/ts/extract/metric/{metric}',
    'get_ts_list': '/metadata/funcId',
    'get_ts_meta': '/ts/tsuid/{tsuid}',
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


class TDMClient(GenericClient):
    """
    Temporal Data Manager api used to connect to JAVA Ikats API
    """

    def __init__(self, *args, **kwargs):
        super(TDMClient, self).__init__(*args, **kwargs)

    def get_ts_list(self):
        """
        Get the list of all TSUID in database

        :return: the list of TSUID with their associated metrics
        :rtype: list
        """
        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['get_ts_list'])

        if response.status_code == 200:
            return response.json
        elif response.status_code == 404:
            return []
        else:
            raise ValueError("No TS found in database")

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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.POST,
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

        :raises TypeError: if name is not a str
        :raises IkatsNotFoundError: if dataset doesn't exist in database
        """

        # Checks inputs
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        ret = {
            'ts_list': [],
            'description': None
        }

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['dataset_read'],
                             uri_params={
                                 'name': name
                             })

        is_404(response=response, msg="Dataset %s not found in database" % name)

        if response.status_code == 200:
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
        check_is_valid_ds_name(value=name, raise_exception=True)
        check_type(value=deep, allowed_types=bool, var_name="deep", raise_exception=True)

        template = 'dataset_remove'
        if deep:
            template = 'dataset_deep_remove'
        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES[template],
                             uri_params={
                                 'name': name
                             })

        if response.status_code == 404:
            raise IkatsNotFoundError("Dataset %s not found in database" % name)
        return response

    def dataset_list(self):
        """
        Get the list of all dataset and their corresponding description

        :return: dataset information :[{'name':name,'description':description}]
        :rtype: list of dict
        """

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['get_data_set_list'])

        results = []

        try:
            # Keep only the necessary fields from the request
            for content in response.json:
                results.append({'name': content['name'], 'description': content['description']})
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
        :raises IkatsConflictError: if *fid* exists
        :raises SystemError: if another issue occurs
        """

        # Checks inputs
        check_is_fid_valid(fid=fid, raise_exception=True)
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)
        if tsuid == "":
            raise ValueError("tsuid must not be empty")

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid,
            'fid': fid
        }

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['import_fid'],
                             uri_params=uri_params)

        # In case of success, web app returns 2XX
        if response.status_code == 200:
            pass
        elif response.status_code == 409:
            raise IkatsConflictError("TSUID:%s - FID already exists (not updated) %s" % (tsuid, fid))
        else:
            self.session.log.warning("TSUID:%s - FID %s not created (got %s)", tsuid, fid, response.status_code)
            raise SystemError("TSUID:%s - FID %s not created (got %s)" % (tsuid, fid, response.status_code))

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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['get_fid'],
                             uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status_code == 200:

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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['delete_fid'],
                             uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            self.session.log.info("TSUID:%s - FID deleted", tsuid)
        else:
            self.session.log.warning("TSUID [%s] - FID not deleted. Received status_code:%s", tsuid,
                                     response.status_code)
            raise ValueError

    def metadata_create(self, tsuid, name, value, data_type=DTYPE.string, force_update=False):
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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['import_meta_data'],
                             uri_params=uri_params,
                             q_params=q_params)

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            return True
        elif response.status_code == 409:
            if force_update:
                # Error occurred (can't create a metadata that already exists - conflict)
                # Try to update it because it is wanted
                return self.metadata_update(tsuid=tsuid, name=name, value=value, force_create=False)
            else:
                self.session.log.warning("TSUID:%s - MetaData already exists (not updated) %s=%s", tsuid, name, value)
                return False
        self.session.log.warning("TSUID [%s] - MetaData not created %s=%s. Received status_code:%s", tsuid, name, value,
                                 response.status_code)
        return False

    def metadata_update(self, tsuid, name, value, data_type=DTYPE.string, force_create=False):
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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.PUT,
                             template=TEMPLATES['update_meta_data'],
                             uri_params=uri_params)

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            if force_create:
                # Error occurred (can't update a metadata that doesn't exists)
                # Try to create it because it is wanted
                return self.metadata_create(tsuid=tsuid, name=name, value=value, data_type=data_type,
                                            force_update=False)
            else:
                self.session.log.warning("TSUID:%s - MetaData doesn't exists and not Created %s=%s", tsuid, name, value)
                return False
        self.session.log.warning(
            "TSUID [%s] - MetaData not updated %s=%s. Received status_code:%s", tsuid, name, value,
            response.status_code)
        return False

    def metadata_get(self, ts_list):
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

            response = self.send(root_url=self.session.tdm_url,
                                 verb=GenericClient.VERB.GET,
                                 template=TEMPLATES['lookup_meta_data'],
                                 q_params=q_params)

            if response.status_code == 414:
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

    def metadata_get_typed(self, ts_list):
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

            response = self.send(root_url=self.session.tdm_url,
                                 verb=GenericClient.VERB.GET,
                                 template=TEMPLATES['lookup_meta_data'],
                                 q_params=q_params)

            if response.status_code == 414:
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

    def get_ts_from_metadata(self, constraint=None):
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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['get_one_functional_identifier'],
                             uri_params={'tsuid': tsuid},
                             data=None,
                             files=None)

        check_http_code(response)

        return response.json['funcId']

    def get_tsuid_from_fid(self, fid):
        """
        Retrieve the tsuid associated to the func_id param.

        :param fid: one func_id value
        :type fid: str

        :return: retrieved tsuid value
        :rtype: str

        :raises TypeError: if unexpected fid parameter
        :raises TypeError: status_code 400 (bad request)
        :raises TypeError: unexpected http status_code
        :raises ValueError: mismatched result: http status_code 404:  not found
        :raises ServerError: http status_code for server errors: 500 <= status_code < 600
        """
        check_is_fid_valid(fid=fid, raise_exception=True)

        # empty result => throws IkatsNotFoundError
        res = self.search_functional_identifiers(criterion_type='funcIds', criteria_list=[fid])

        assert (isinstance(res, list)), "get_tsuid_from_func_id: failed to retrieve json result as list"
        assert (isinstance(res[0], dict)), "get_tsuid_from_func_id: failed to retrieve first item from result list"
        return res[0]['tsuid']

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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['search_functional_identifier_list'],
                             uri_params=None,
                             data=my_filter,
                             files=None)
        check_http_code(response)

        return response.json

    def create_table(self, data):
        """
        Create a table

        :param data: data to store
        :type data: dict

        :return: the name of the created table

        :raises IkatsInputError: for any error present in the inputs
        :raises IkatsException: for any other error during the request
        """

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['create_table'],
                             json_data=data,
                             files=None)

        if response.status_code == 400:
            err_msg = 'Bad request while creating Table %s produced: %s' % (response.url, response.json)
            raise IkatsInputError(err_msg)
        elif response.status_code == 409:
            err_msg = 'Conflict detected while creating Table %s produced: %s' % (response.url, response.json)
            raise IkatsConflictError(err_msg)
        elif response.status_code >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status_code,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise IkatsServerError(err_msg)

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
        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['list_tables'],
                             uri_params={'name': name, 'strict': strict},
                             data=None,
                             files=None)

        if response.status_code == 404:
            return response.json
        elif response.status_code >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status_code,
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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['read_table'],
                             uri_params={'name': name},
                             data=None,
                             files=None)

        if response.status_code == 400:
            raise IkatsInputError("Wrong input: [%s]" % name)
        if response.status_code == 404:
            raise IkatsNotFoundError("Table %s not found" % name)
        elif response.status_code >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status_code,
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

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['delete_table'],
                             uri_params={'name': name})

        if response.status_code == 400:
            raise IkatsInputError("Wrong input: [%s]" % name)
        if response.status_code == 404:
            raise IkatsNotFoundError("Table %s not found" % name)
        elif response.status_code >= 500:
            err_msg = "%s (unexpected status_code here) %s produced: %s" % (response.status_code,
                                                                            response.url,
                                                                            response.json)
            self.session.log.error(err_msg)
            raise IkatsException(err_msg)

    def remove_ts(self, tsuid, raise_exception=True):
        """
        Remove timeseries from base
        return bool status except if raise_exception is set to True.
        In this case, return True or raise the corresponding exception

        Corresponding web app resource operation: **removeTimeSeries**
        which deletes also all information related to other objects like Metadata, Dataset

        :param tsuid: tsuid of the timeseries to remove
        :param raise_exception: set to True to raise exceptions

        :type tsuid: str
        :type raise_exception: bool

        :return: the action status (True if deleted, False otherwise)
        :rtype: bool


        :raises TypeError: if *tsuid* is not a str
        :raises IkatsNotFoundError: if *tsuid* is not found on server
        :raises IkatsConflictError: if *tsuid* belongs to -at least- one dataset
        :raises SystemError: if any other unhandled error occurred
        """

        # Checks inputs
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)

        # List of items to be replaced by in the template
        uri_params = {
            'tsuid': tsuid
        }

        response = self.send(root_url=self.session.tdm_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['remove_ts'],
                             uri_params=uri_params)

        if response.status_code == 204:
            # Timeseries has been successfully deleted
            result = True
        elif response.status_code == 404:
            # Timeseries not found in database
            if raise_exception:
                raise IkatsNotFoundError("Timeseries %s not found in database" % tsuid)
            else:
                result = False
        elif response.status_code == 409:
            # Timeseries linked to existing dataset
            if raise_exception:
                raise IkatsConflictError("%s belongs to -at least- one dataset" % tsuid)
            else:
                result = False
        else:
            if raise_exception:
                raise SystemError("An unhandled error occurred")
            else:
                result = False
        return result

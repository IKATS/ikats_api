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

from ikats.client.generic_client import (GenericClient, check_http_code,
                                         is_4xx, is_5xx, is_400, is_404,
                                         is_409)
from ikats.exceptions import (IkatsConflictError, IkatsException,
                              IkatsNotFoundError)
from ikats.lib import (MDType, check_is_fid_valid, check_is_valid_ds_name,
                       check_type)

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
    'metadata_delete': '/metadata/{tsuid}/{name}',
    'import_meta_data_file': '/metadata/import/file',
    'dataset_create': '/dataset/import/{data_set}',
    'dataset_read': '/dataset/{name}',
    'get_data_set_list': '/dataset',
    'dataset_remove': '/dataset/{name}',
    'dataset_deep_remove': '/dataset/{name}?deep=true',
    'search': '/ts/lookup/{metric}',
    'ts_match': '/metadata/tsmatch',
    'get_one_functional_identifier': '/metadata/funcId/{tsuid}',
    'search_functional_identifier_list': '/metadata/funcId',
    'pid_read': '/processdata/{pid}',
    'rid_read': '/processdata/id/download/{rid}',
    'rid_add': '/processdata?name={name}&processId={process_id}',
    'rid_delete': '/processdata/{rid}',
    'table_delete': '/table/{name}',
    'table_read': '/table/{name}',
    'table_list': '/table',
    'create_table': '/table',
}


class DatamodelClient(GenericClient):
    """
    Temporal Data Manager api used to connect to JAVA Ikats API
    """

    def __init__(self, *args, **kwargs):
        super(DatamodelClient, self).__init__(*args, **kwargs)
        self.root_url = "/TemporalDataManagerWebApp/webapi"

    def get_ts_list(self):
        """
        Get the list of all TSUID from database

        :returns: the list of TSUID with their fid
        :rtype: list
        """
        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['get_ts_list'])

        if response.status_code == 200:
            return response.json
        if response.status_code == 404:
            return []
        raise ValueError("No TS found in database")

    def dataset_create(self, name, description, ts):
        """
        Create a new dataset composed of the *tsuid_list*

        Corresponding web app resource operation: **importDataSet**

        :param name: name of the dataset
        :param description: short functional description about the content
        :param ts: list of tsuid composing the dataset

        :type name: str
        :type description: str
        :type ts: list

        :returns: execution status_code (True if success, False otherwise)
        :rtype: bool

        :raises TypeError: if *tsuid_list* is not a list
        :raises IkatsConflictError: if *name* already exists in database
        """

        # Inputs check
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)
        check_type(value=description, allowed_types=str, var_name="description", raise_exception=True)
        check_type(value=ts, allowed_types=list, var_name="ts", raise_exception=True)

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['dataset_create'],
                             uri_params={
                                 'data_set': name,
                             },
                             data={
                                 'name': name,
                                 'description': description,
                                 'tsuidList': ','.join(ts),
                             })

        if response.status_code == 409:
            raise IkatsConflictError("Dataset %s already exists in database" % name)

    def dataset_read(self, name):
        """
        Retrieve the details of a Dataset provided in arguments

        Corresponding web app resource operation: **getDataSet**

        :param name: name of the dataset to request TS list from
        :type name: str

        :return:
           information about ts_list and description
           * *ts_list* is the list of TS matching the data_set
           * *description* is the description sentence of the dataset
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

        response = self.send(root_url=self.session.dm_url + self.root_url,
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
        raise SystemError("Something wrong happened")

    def dataset_delete(self, name, deep=False):
        """
        Remove data_set from base

        Corresponding web app resource operation: **removeDataSet**

        :param name: name of the dataset to delete
        :type name: str

        :param deep: true to deeply remove dataset (TSUID and metadata erased)
        :type deep: boolean

        :returns: the response body
        :rtype: str

        .. note::
           Removing an unknown dataset results in a successful operation (server constraint)
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
        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES[template],
                             uri_params={
                                 'name': name
                             })

        if response.status_code == 404:
            raise IkatsNotFoundError("Dataset %s not found in database" % name)
        return response.text

    def dataset_list(self):
        """
        Get the list of all dataset and their corresponding description

        :returns: dataset information :[{'name':name,'description':description}]
        :rtype: list of dict
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
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
        Import a functional ID into database

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

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['import_fid'],
                             uri_params={
                                 'tsuid': tsuid,
                                 'fid': fid
                             }
                             )

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
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['get_fid'],
                             uri_params={
                                 'tsuid': tsuid
                             })

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            if response.json != '{}':
                fid = response.json['funcId']
                return fid
            raise IndexError("No FID for TSUID [%s]" % tsuid)
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
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['delete_fid'],
                             uri_params={
                                 'tsuid': tsuid
                             })

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            self.session.log.info("TSUID:%s - FID deleted", tsuid)
        else:
            self.session.log.warning("TSUID [%s] - FID not deleted. Received status_code:%s", tsuid,
                                     response.status_code)
            raise ValueError

    def metadata_create(self, tsuid, name, value, data_type=MDType.STRING, force_update=False):
        """
        Import a metadata into database

        Corresponding web app resource operation: **importMetaData**

        :param tsuid: Functional Identifier of the TS
        :param name: Metadata name
        :param value: Value of the metadata
        :param data_type: data type of the metadata
        :param force_update: True to create the meta if not exists (default: False)

        :type tsuid: str
        :type name: str
        :type value: str or number
        :type data_type: MDType
        :type force_update: bool

        :returns: execution status_code, True if import successful, False otherwise
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
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)
        check_type(value=value, allowed_types=[str, int, float], var_name="value", raise_exception=True)
        check_type(value=data_type, allowed_types=MDType, var_name="data_type", raise_exception=True)
        check_type(value=force_update, allowed_types=bool, var_name="force_update", raise_exception=True)

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")
        if name == "":
            self.session.log.error("name must not be empty")
            raise ValueError("name must not be empty")
        if value == "":
            self.session.log.error("value must not be empty")
            raise ValueError("value must not be empty")

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['import_meta_data'],
                             uri_params={
                                 'tsuid': tsuid,
                                 'name': name,
                                 'value': value
                             },
                             q_params={
                                 'dtype': data_type.value
                             })

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            return True
        if response.status_code == 409:
            if force_update:
                # Error occurred (can't create a metadata that already exists - conflict)
                # Try to update it because it is wanted
                return self.metadata_update(tsuid=tsuid, name=name, value=value, force_create=False)
            raise IkatsConflictError("Can't set metadata %s to %s (for tsuid %s)" % (name, value, tsuid))
        return False

    def metadata_update(self, tsuid, name, value, data_type=MDType.STRING, force_create=False):
        """
        Import a metadata into database

        Corresponding web app resource operation: **importMetaData**

        :param tsuid: Functional Identifier of the TS
        :param name: Metadata name
        :param value: Value of the metadata
        :param data_type: data type of the metadata (used only if force_create, no type change on existing metadata)
        :param force_create: True to create the meta if not exists (default: False)

        :type tsuid: str
        :type name: str
        :type value: str or number
        :type data_type: MDType
        :type force_create: bool

        :returns: execution status_code, True if import successful, False otherwise
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
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)
        check_type(value=value, allowed_types=[str, int, float], var_name="value", raise_exception=True)
        check_type(value=force_create, allowed_types=bool, var_name="force_create", raise_exception=True)
        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")
        if name == "":
            self.session.log.error("name must not be empty")
            raise ValueError("name must not be empty")
        if value == "":
            self.session.log.error("value must not be empty")
            raise ValueError("value must not be empty")

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.PUT,
                             template=TEMPLATES['update_meta_data'],
                             uri_params={
                                 'tsuid': tsuid,
                                 'name': name,
                                 'value': value,
                             })

        # in case of success, web app returns 2XX
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            if force_create:
                # Error occurred (can't update a metadata that doesn't exists)
                # Try to create it because it is wanted
                return self.metadata_create(tsuid=tsuid, name=name, value=value, data_type=data_type,
                                            force_update=False)
            self.session.log.warning("TSUID:%s - MetaData doesn't exists and not Created %s=%s", tsuid, name, value)
            return False
        self.session.log.warning(
            "TSUID [%s] - MetaData not updated %s=%s. Received status_code:%s", tsuid, name, value,
            response.status_code)
        return False

    def metadata_delete(self, tsuid, name, raise_exception=True):
        """
        Delete a metadata

        Corresponding web app resource operation: **removeMetaData**

        :param tsuid: TSUID of the Timeseries where is bound the metadata
        :param name: Metadata name
        :param raise_exception: (optional) Indicates if Ikats exceptions shall be raised (True, default) or not (False)

        :type tsuid: str
        :type name: str
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *tsuid* not a str
        :raises TypeError: if *name* not a str

        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *name* is empty
        :raises IkatsNotFoundError: if metadata doesn't exist
        """

        # Checks inputs
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        if tsuid == "":
            self.session.log.error("tsuid must not be empty")
            raise ValueError("tsuid must not be empty")
        if name == "":
            self.session.log.error("name must not be empty")
            raise ValueError("name must not be empty")

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['metadata_delete'],
                             uri_params={
                                 'tsuid': tsuid,
                                 'name': name,
                             })

        try:
            is_404(response, "Metadata '%s' not found for TS '%s'" % (name, tsuid))
            is_4xx(response, msg="Unexpected client error : {code}")
            is_5xx(response, msg="Unexpected server error : {code}")
        except IkatsException:
            if raise_exception:
                raise
            return False

        return True

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

        :param ts_list: list of TSUID identifier
        :type ts_list: str or list

        :returns: metadata for each TS
        :rtype: dict (key is TS identifier, value is list of metadata)
            | {
            |     'TS1': {'param1':'value1', 'param2':'value2'},
            |     'TS2': {'param1':'value1', 'param2':'value2'}
            | }

        :raises TypeError: if *ts_list* is neither a str nor a list
        """

        # Checks inputs
        check_type(value=ts_list, allowed_types=[list, str], var_name="ts_list", raise_exception=True)
        if isinstance(ts_list, str):
            # Hack to convert string to list (to homogenize treatment)
            ts_list = ts_list.split(',')

        output_dict = {}

        # It is not possible to have infinite URL length using GET method
        # We have to divide in 'chunks' to not exceed the URL size limit.
        # Commonly, this size is 8KB long (8192 chars)
        # The chunk_size is set to a value which approach this limit with a safety coeff
        chunk_size = 100
        for i in range(0, len(ts_list), chunk_size):
            working_ts_list = ts_list[i:i + chunk_size]

            response = self.send(root_url=self.session.dm_url + self.root_url,
                                 verb=GenericClient.VERB.GET,
                                 template=TEMPLATES['lookup_meta_data'],
                                 q_params={'tsuid': ','.join(working_ts_list)})

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

        :param ts_list: list of TS identifier
        :type ts_list: str or list

        :returns: metadata for each TS
        :rtype: dict (key is TS identifier, value is list of metadata with its associated data type)
            | {
            |     'TS1': {'param1':{'value':'value1', 'dtype': 'dtype'}, 'param2':{'value':'value2', 'dtype': 'dtype'}},
            |     'TS2': {'param1':{'value':'value1', 'dtype': 'dtype'}, 'param2':{'value':'value2', 'dtype': 'dtype'}}
            | }

        :raises TypeError: if *ts_list* is neither a str nor a list
        """

        # Checks inputs
        check_type(value=ts_list, allowed_types=[list, str], var_name="ts_list", raise_exception=True)
        if isinstance(ts_list, str):
            # Hack to convert string to list (to homogenize treatment)
            ts_list = ts_list.split(',')

        output_dict = {}

        # It is not possible to have infinite URL length using GET method
        # We have to divide in 'chunks' to not exceed the URL size limit.
        # Commonly, this size is 8KB long (8192 chars)
        # The chunk_size is set to a value which approach this limit with a safety coeff
        chunk_size = 100
        for i in range(0, len(ts_list), chunk_size):
            working_ts_list = ts_list[i:i + chunk_size]

            response = self.send(root_url=self.session.dm_url + self.root_url,
                                 verb=GenericClient.VERB.GET,
                                 template=TEMPLATES['lookup_meta_data'],
                                 q_params={'tsuid': ','.join(working_ts_list)})

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
            #           'unit':{'value':'meters', 'dtype':'string'},
            #           'FlightPhase': {'value':'TakeOff', 'dtype':'string'},
            #       }
            #   }

            # init the output with ts list as keys
            for ts in working_ts_list:
                output_dict[ts] = {}

            # No result in metadata, return directly the empty list
            if response.json == "{}":
                return output_dict

            # Fill in metadata for each ts
            for content in response.json:

                # Init the key if first meet
                if content['tsuid'] not in output_dict:
                    output_dict[content['tsuid']] = {}

                output_dict[content['tsuid']][content['name']] = {
                    'value': content['value'],
                    'dtype': MDType(content['dtype'])
                }

        return output_dict

    def get_ts_from_metadata(self, constraint=None):
        """
        From a metadata constraint provided in parameter, the method get a TS list matching these constraints

        Corresponding web app resource operation: **TSMatch**

        Example of constraint:
            | {
            |     frequency: [1, 2],
            |     flight_phase: 8
            | }
        will find the TS having the following metadata:
            | (frequency == 1 OR frequency == 2)
            | AND
            | flight_phase == 8

        :param constraint: constraint definition
        :type constraint: dict

        :returns: list of TSUID matching the constraints
        :rtype: dict

        :raises TypeError: if *constraint* is not a dict
        """

        # Checks inputs
        if constraint is None:
            constraint = {}
        check_type(value=constraint, allowed_types=dict, var_name="constraint", raise_exception=True)

        response = self.send(root_url=self.session.dm_url + self.root_url,
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

        :returns: retrieved functional identifier resource
        :rtype: dict having following keys defined:
          - 'tsuid'
          - and 'funcId'

        :raises exception:
            - TypeError: if tsuid is not a str OR status_code 400 (bad request) OR unexpected http status_code
            - ValueError: mismatched result: http status_code 404:  not found
            - ServerError: http status_code for server errors: 500 <= status_code < 600
        """
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)

        response = self.send(root_url=self.session.dm_url + self.root_url,
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

        :returns: retrieved tsuid value
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

        :returns: matching list of functional identifier resources: dict having following keys defined:
            - 'tsuid',
            - and 'funcId'
        :rtype: list of dict

        :raises exception:
          - TypeError: if unexpected arguments OR status_code 400 (bad request) OR unexpected http status_code
          - ValueError: mismatched result: http status_code 404:  not found
          - ServerError: http status_code for server errors: 500 <= status_code < 600
        """
        check_type(value=criterion_type, allowed_types=str, var_name="criterion_type", raise_exception=True)
        check_type(value=criteria_list, allowed_types=list, var_name="criteria_list", raise_exception=True)

        my_filter = dict()
        my_filter[criterion_type] = criteria_list

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['search_functional_identifier_list'],
                             data=my_filter,
                             files=None)
        check_http_code(response)

        return response.json

    def table_create(self, data):
        """
        Create a table

        :param data: data to store
        :type data: dict

        :returns: the name of the created table
        :rtype: str

        :raises IkatsInputError: for any error present in the inputs
        :raises IkatsConflictError: if table already exist
        :raises IkatsException: for any other error during the request
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES['create_table'],
                             json_data=data,
                             files=None)

        is_400(response=response, msg=response.json)
        is_409(response=response, msg="Table %s already exist in database" % data['table_desc']['name'])
        is_4xx(response, "Unexpected client error : {code}")
        is_5xx(response, "Unexpected server error : {code}")

        return response.json

    def table_list(self, name=None, strict=True):
        """
        List all tables
        If name is specified, filter by name
        name can contains "*", this character is considered as "any chars" (equivalent to regexp /.*/)

        :param name: name to find
        :param strict: consider name without any wildcards

        :type name: str or None
        :type strict: bool

        :returns: the list of tables matching the requirements
        :rtype: list

        :raises IkatsInputError: for any error present in the inputs
        :raises IkatsException: for any other error during the request
        """
        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['table_list'],
                             uri_params={'name': name, 'strict': strict},
                             data=None,
                             files=None)

        is_5xx(response, "Unexpected server error : {code}")

        return response.json

    def table_read(self, name):
        """
        Reads the data blob content: for the unique table identified by id.

        :param name: the name of the raw table to get data from
        :type name: str

        :returns: the content data stored.
        :rtype: dict

        :raises IkatsNotFoundError: no resource identified by ID
        :raises IkatsException: any other error
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['table_read'],
                             uri_params={'name': name},
                             data=None,
                             files=None)

        is_400(response=response, msg="Wrong input: [%s]" % name)
        is_404(response=response, msg="Table %s not found" % name)
        is_4xx(response, "Unexpected client error : {code}")
        is_5xx(response, "Unexpected server error : {code}")

        return response.json

    def table_delete(self, name):
        """
        Delete a table

        :param name: the name of the table to delete
        :type name: str
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['table_delete'],
                             uri_params={'name': name})

        is_400(response, msg="Wrong input: [%s]" % name)
        is_404(response, msg="Table %s not found" % name)
        is_4xx(response, "Unexpected client error : {code}")
        is_5xx(response, "Unexpected server error : {code}")

    def ts_delete(self, tsuid, raise_exception=True):
        """
        Remove timeseries from database
        return bool status except if raise_exception is set to True.
        In this case, return True or raise the corresponding exception

        Corresponding web app resource operation: **removeTimeSeries**
        which deletes also all information related to other objects like Metadata, Dataset

        :param tsuid: tsuid of the timeseries to remove
        :param raise_exception: set to True to raise exceptions

        :type tsuid: str
        :type raise_exception: bool

        :returns: the action status (True if deleted, False otherwise)
        :rtype: bool

        :raises TypeError: if *tsuid* is not a str
        :raises IkatsNotFoundError: if *tsuid* is not found on server
        :raises IkatsConflictError: if *tsuid* belongs to -at least- one dataset
        :raises SystemError: if any other unhandled error occurred
        """

        # Checks inputs
        check_type(value=tsuid, allowed_types=str, var_name="tsuid", raise_exception=True)

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['remove_ts'],
                             uri_params={
                                 'tsuid': tsuid
                             })

        if response.status_code == 204:
            # Timeseries has been successfully deleted
            result = True
        elif response.status_code == 404:
            # Timeseries not found in database
            if raise_exception:
                raise IkatsNotFoundError("Timeseries %s not found in database" % tsuid)
            result = False
        elif response.status_code == 409:
            # Timeseries linked to existing dataset
            if raise_exception:
                raise IkatsConflictError("%s belongs to -at least- one dataset" % tsuid)
            result = False
        else:
            if raise_exception:
                raise SystemError("An unhandled error occurred")
            result = False
        return result

    def pid_results(self, pid):
        """
        Get a list of the results (RID) associated to PID

        :param pid: process ID to get
        :type pid: str or int

        :return: the result
        :rtype: list
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['pid_read'],
                             uri_params={"pid": pid})

        # See bugs #2780
        # This is a workaround
        if not response.data:
            raise IkatsNotFoundError("No RID found for PID:%s" % pid)

        return response.data

    def rid_get(self, rid):
        """
        Get a specific result

        :param rid: result ID to get
        :type rid: int

        :return: the result
        :rtype: dict
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['rid_read'],
                             uri_params={"rid": rid})

        is_404(response=response, msg="RID not found :%s" % rid)
        return response.data

    def rid_create(self, data, pid, name=None):
        """
        Push new data to be considered as a result of the run identified by *pid*

        :param pid: process id to store information to
        :param data: data to store
        :param name: (option) label of the data

        :type pid: str or int
        :type data: object
        :type name: str or None

        :return: the RID of created result
        :rtype: int
        """

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.POST,
                             template=TEMPLATES["rid_add"],
                             data=data,
                             uri_params={
                                 'process_id': pid,
                                 'name': name,
                             })
        is_4xx(response, "Unexpected client error : {code}")
        is_5xx(response, "Unexpected server error : {code}")

        try:
            rid = int(response.text)
            return rid
        except ValueError:
            raise IkatsException("response couldn't be parsed correctly %s" % response)

    def rid_delete(self, rid, raise_exception=True):
        """
        Delete a metadata

        Corresponding web app resource operation: **removeMetaData**

        :param rid: Result ID of the data to delete
        :param raise_exception: (optional) Indicates if Ikats exceptions shall be raised (True, default) or not (False)

        :type rid: str or int
        :type raise_exception: bool

        :returns: the status of the action
        :rtype: bool

        :raises TypeError: if *rid* not a str nor int

        :raises IkatsNotFoundError: if *rid* doesn't exist
        """

        # Checks inputs
        check_type(value=rid, allowed_types=[str, int], var_name="rid", raise_exception=True)

        response = self.send(root_url=self.session.dm_url + self.root_url,
                             verb=GenericClient.VERB.DELETE,
                             template=TEMPLATES['rid_delete'],
                             uri_params={
                                 'rid': rid
                             })

        try:
            is_404(response, "RID %s not found" % rid)
            is_4xx(response, msg="Unexpected client error : {code}")
            is_5xx(response, msg="Unexpected server error : {code}")
        except IkatsException:
            if raise_exception:
                raise
            return False
        return True

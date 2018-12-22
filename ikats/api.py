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
from ikats import IkatsSession, Dataset, Timeseries
from ikats.exception import IkatsException, IkatsNotFoundError
from ikats.client import TemporalDataMgr, NonTemporalDataMgr
from ikats.client.temporal_data_mgr import DTYPE
from ikats.client.opentsdb import OpenTSDB


class IkatsGenericApiEndPoint(object):
    """
    Abstract Ikats End Point class
    The 5 API endpoints types are :
    - Create
    - Read
    - Update
    - Delete
    - List
    """

    def __init__(self, session):
        self.__session = None
        self.session = session

    @property
    def session(self):
        return self.__session

    @session.setter
    def session(self, value):
        if type(value) == IkatsSession:
            self.__session = value


# noinspection PyMethodOverriding,PyAbstractClass
class IkatsProcessDataMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Process Data management
    """

    def __init__(self, *args, **kwargs):
        super(IkatsProcessDataMgr, self).__init__(*args, **kwargs)

    @staticmethod
    def create(data, process_id, name, data_type=None):
        """
        Create a process data

        :param data: data to store
        :param process_id: id of the process to bind this data to
        :param name: name of the process data
        :param data_type: data_type (deprecated) of the data to store

        :type data: any
        :type process_id: str or int
        :type name: str
        :type data_type: str or None

        :return: execution status

        :raise ValueError: if data_type is not handled
        :raise TypeError: data content type is not handled
        """

        ntdm = NonTemporalDataMgr()
        return ntdm.add_data(data=data, process_id=process_id, data_type=data_type, name=name)

    @staticmethod
    def list(process_id):
        """
        Reads the ProcessData resource list matching the process_id.

        :param process_id: processId of the filtered resources. It can be the ID of an executable algorithm,
          or any other data producer identifier.
        :type process_id: int or str

        :return: the data without BLOB content: list of resources matching the process_id.
           Each resource is a dict mapping the ProcessData json from RestFul API: dict with entries:
             - id
             - processId
             - name
             - dataType

        :rtype: list
        """
        ntdm = NonTemporalDataMgr()
        return ntdm.get_data(process_id=process_id)

    @staticmethod
    def read(process_data_id):
        """
        Reads the data blob content: for the unique process_data row identified by id.

        :param process_data_id: the id key of the raw process_data to get data from

        :return: the content data stored.
        :rtype: bytes or str or object

        :raise IkatsNotFoundError: no resource identified by ID
        :raise IkatsException: failed to read
        """
        response = None
        try:
            ntdm = NonTemporalDataMgr()
            response = ntdm.download_data(process_data_id)

            if response.status == 200:
                # Reads the data returned by the service.

                # default_content is only used if the content_type is unknown by RestClientResponse
                return response.get_appropriate_content(default_content=response.text)

            elif response.status == 404:
                msg = "IkatsProcessData::read({}) resource not found : HTTP response={}"
                raise IkatsNotFoundError(msg.format(process_data_id, response))
            else:
                msg = "IkatsProcessData::read({}) failed : HTTP response={}"
                raise IkatsException(msg.format(process_data_id, response))

        except IkatsException:
            raise

        except Exception as exception:
            msg = "IkatsProcessData::read({}) failed : unexpected error. got response={}"
            raise IkatsException(msg.format(exception, response))

    @staticmethod
    def delete(process_id):
        """
        Delete a process data

        :param process_id: the process data id to delete
        :type process_id: int or str

        :return: the status of deletion (True=deleted, False otherwise)
        :rtype: bool
        """
        ntdm = NonTemporalDataMgr()
        ntdm.remove_data(process_id=process_id)


# noinspection PyMethodOverriding,PyAbstractClass
class IkatsTableMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Table management
    """

    def __init__(self, *args, **kwargs):
        super(IkatsTableMgr, self).__init__(*args, **kwargs)

    @staticmethod
    def create(data, name=None, description=None):
        """
        Create a table

        If name or description is provided,
        the method will overwrite the corresponding fields inside the data.

        :param data: data to store
        :param name: name of the table (optional)
        :param description: description of the table (optional)

        :type data: dict
        :type name: str or None
        :type description: str or None

        :return: the id of the created table
        """
        if name is not None:
            data['table_desc']['name'] = name
        if description is not None:
            data['table_desc']['desc'] = description
        tdm = TemporalDataMgr()
        return tdm.create_table(data=data)

    @staticmethod
    def list(name=None, strict=True):
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
        """
        tdm = TemporalDataMgr()
        return tdm.list_tables(name=name, strict=strict)

    @staticmethod
    def read(name):
        """
        Reads the data blob content: for the unique table identified by id.

        :param name: the id key of the raw table to get data from
        :type name: str

        :return: the content data stored.
        :rtype: bytes or str or object

        :raise IkatsNotFoundError: no resource identified by ID
        :raise IkatsException: any other error
        """

        tdm = TemporalDataMgr()
        return tdm.read_table(name=name)

    @staticmethod
    def delete(name):
        """
        Delete a table

        :param name: the name of the table to delete
        :type name: str

        :return: the status of deletion (True=deleted, False otherwise)
        :rtype: bool
        """
        tdm = TemporalDataMgr()
        return tdm.delete_table(name=name)

    @staticmethod
    def extract(table_content, obs_id, items):
        """
        Extract information from a table and format the output as a dict of dict
        The first key will be the obs_id values taken from the table_content.
        The sub keys will be the items

        :param table_content: the JSON content corresponding to the table as dict
        :param obs_id: Column name used as primary key
        :param items: list of other columns to extract

        :type table_content: dict
        :type obs_id: str
        :type items: list

        :return: a dict of dict where first key is the obs_id and the sub keys are the items
        :rtype: dict
        """

        # 2D array containing the equivalent of the rendered JSON structure
        data_array = []

        try:
            # Get the columns name with a mapping dict
            columns_name = {k: v for v, k in enumerate(table_content["headers"]["col"]["data"])}
        except:
            raise ValueError("Table content shall contain col headers to know the name of columns")

        try:
            # Fill the 2D array with the content of the header column
            # Skip the first cell by starting at index 1
            data_array = [[x] for x in table_content["headers"]["row"]["data"][1:]]
        except KeyError:
            # No header column present, skip it
            pass

        # Building final computed results
        results = {}
        for line_index, line in enumerate(table_content["content"]["cells"]):
            if len(data_array) < line_index:
                # Fill in the data_array line with an empty list in case there was no header column
                data_array.append([])
            # Extend the current column with the other columns
            data_array[line_index].extend(line)

            first_key_value = data_array[line_index][columns_name[obs_id]]

            if first_key_value in results:
                raise ValueError("Key %s is not unique", obs_id)

            results[first_key_value] = {}
            for item in items:
                results[first_key_value][item] = data_array[line_index][columns_name[item]]
        return results


# noinspection PyAbstractClass,PyMethodOverriding
class IkatsTimeseriesMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to TimeSeries management
    """

    def __init__(self, *args, **kwargs):
        super(IkatsTimeseriesMgr, self).__init__(*args, **kwargs)

    def create_ref(self, fid):
        """
        Create a reference of timeseries in temporal data database and associate it to fid
        in non temporal database for future use.
        Shall be used before create method in case of parallel creation of data (import data via spark for example)

        :param fid: Functional Identifier of the TS in Ikats
        :type fid: str

        :return: the timeseries reference in database (tsuid)
        :rtype: str
        """
        return OpenTSDB(ikats_session=self.session).create_tsuid(fid=fid)

    @staticmethod
    def create(fid, data=None, generate_metadata=True, parent=None, *args, **kwargs):
        """
        Import TS data points in database or update an existing TS with new points

        :param data: array of points where first column is timestamp (EPOCH ms) and second is value (float compatible)
        :param fid: Functional Identifier of the TS in Ikats
        :param parent: optional, default None: TSUID of inheritance parent
        :param generate_metadata: Generate metadata (useful when doing partial import) (Default: True)

        :type data: ndarray or list
        :type fid: str
        :param parent: str
        :type generate_metadata: bool

        :return: an object containing several information about the import
        :rtype: dict
        """
        return OpenTSDB.import_ts(fid=fid, data=data, generate_metadata=generate_metadata, parent=parent, *args,
                                  **kwargs)

    @staticmethod
    def inherit(tsuid, parent, *args, **kwargs):
        """
        Make a time series inherit of parent's metadata according to a pattern (not all metadata inherited)

        :param tsuid: TSUID of the TS in Ikats (which will inherit)
        :param parent: TSUID of inheritance parent

        :type tsuid: str
        :param parent: str
        """
        return OpenTSDB.inherit_properties(tsuid=tsuid, parent=parent, *args, **kwargs)

    @staticmethod
    def read(tsuid_list, sd=None, ed=None):
        """
        Retrieve the data corresponding to a ts (or a list of ts) without knowing date range

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
        :type sd: int
        :type ed: int

        :returns: a list of ts data as numpy array
        :rtype: list of numpy array

        :raises TypeError: if *tsuid_list* is neither a list nor a string
        """
        tdm = TemporalDataMgr()
        return tdm.get_ts(tsuid_list=tsuid_list, sd=sd, ed=ed)

    @staticmethod
    def delete(tsuid, no_exception=False):
        """
        Delete the data corresponding to a ts and all associated metadata
        If timeseries belongs to a dataset it will not be removed

        Setting no_exception to True will prevent any exception to be raised.
        Useful to just try to delete a TS if it exists but have no specific action to perform in case of error

        :param tsuid: tsuid of the timeseries to remove
        :type tsuid: str

        :param no_exception: Set to True to not raise any exception, False by default
        :type no_exception: bool

        :raises TypeError: if *tsuid* is not a str
        :raises IkatsNotFoundError: if *tsuid* is not found on server
        :raises IkatsConflictError: if *tsuid* belongs to -at least- one dataset
        :raises SystemError: if any other unhandled error occurred
        """
        tdm = TemporalDataMgr()
        try:
            tdm.remove_ts(tsuid=tsuid)
        except Exception:
            if no_exception:
                pass
            else:
                raise

    @staticmethod
    def list():
        """
        Get the list of all TSUID in database

        :return: the list of TSUID with their associated metrics
        :rtype: list
        """

        tdm = TemporalDataMgr()
        return tdm.get_ts_list()

    @staticmethod
    def find_from_meta(constraint=None):
        """
        From a meta data constraint provided in parameter, the method get a TS list matching these constraints

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

        tdm = TemporalDataMgr()
        return tdm.get_ts_from_meta_data(constraint=constraint)

    @staticmethod
    def fid(tsuid):
        """
        Retrieve the functional ID associated to the tsuid param.
        :param tsuid: one tsuid value
        :type tsuid: str

        :return: retrieved functional identifier value
        :rtype: str

        :raises TypeError:  if tsuid is not a defined str
        :raises ValueError: no functional ID matching the tsuid
        :raises ServerError: http answer with status : 500 <= status < 600
        """

        tdm = TemporalDataMgr()
        return tdm.get_func_id_from_tsuid(tsuid=tsuid)

    @staticmethod
    def nb_points(tsuid, ed=None):
        """
        return the effective imported number of points for a given tsuid

        :param tsuid: timeseries reference in db
        :param ed: end date of the ts to get (EPOCH ms)

        :type tsuid: str
        :type ed: int

        :return: the imported number of points
        :rtype: int

        :raises ValueError: if no TS with tsuid were found
        :raises SystemError: if openTSDB triggers an error
        """

        return OpenTSDB.get_nb_points_from_tsuid(tsuid=tsuid, ed=ed)


# noinspection PyMethodOverriding,PyAbstractClass
class IkatsFunctionalIdentifier(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Functional Identifier management
    """

    @staticmethod
    def create(tsuid, fid):
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

        tdm = TemporalDataMgr()
        tdm.import_fid(tsuid=tsuid, fid=fid)

    @staticmethod
    def read(tsuid):
        """
        Get a functional ID from its TSUID

        :param tsuid: TSUID identifying the TS
        :type tsuid: str

        :raises TypeError: if *tsuid* not a str
        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *tsuid* doesn't have a FID
        """

        tdm = TemporalDataMgr()
        return tdm.get_fid(tsuid=tsuid)

    @staticmethod
    def delete(tsuid):
        """
        Delete a functional ID from its TSUID

        :param tsuid: TSUID identifying the TS
        :type tsuid: str

        :raises TypeError: if *tsuid* not a str
        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if FID not deleted
        """

        tdm = TemporalDataMgr()
        tdm.delete_fid(tsuid=tsuid)

    @staticmethod
    def tsuid(fid):
        """
        Retrieve the tsuid associated to the func_id param.

        :param fid: one func_id value
        :type fid: str

        :return: retrieved tsuid value
        :rtype: str

        :raises TypeError: if unexpected func_id parameter OR status 400 (bad request) OR unexpected http status
        :raises ValueError: mismatched result: http status 404:  not found
        :raises ServerError: http status for server errors: 500 <= status < 600
        """

        tdm = TemporalDataMgr()
        return tdm.get_tsuid_from_func_id(func_id=fid)

    @staticmethod
    def find(criterion_type, criteria_list):
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
          - TypeError: if unexpected arguments OR status 400 (bad request) OR unexpected http status
          - ValueError: mismatched result: http status 404:  not found
          - ServerError: http status for server errors: 500 <= status < 600
        """

        tdm = TemporalDataMgr()
        return tdm.search_functional_identifiers(criterion_type=criterion_type, criteria_list=criteria_list)


# noinspection PyMethodOverriding,PyAbstractClass
class IkatsMetadataMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Metadata management
    """

    @classmethod
    def create(cls, tsuid, name, value, data_type=DTYPE.string, force_update=False):
        """
        Import a meta data into TemporalDataManager

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

        :return: execution status, True if import successful, False otherwise
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

        tdm = TemporalDataMgr()
        result = tdm.import_meta_data(tsuid=tsuid, name=name, value=value, data_type=data_type,
                                      force_update=force_update)
        if not result:
            cls.LOGGER.error("Metadata '%s' couldn't be saved for TS %s", name, tsuid)
        return result

    @staticmethod
    def read(ts_list, with_type=False):
        """
        Request for metadata of a TS or a list of TS

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
        :rtype: dict (key is TS identifier, value is list of metadata without its associated data type)
            | {
            |     'TS1': {'param1':'value1', 'param2':'value2'},
            |     'TS2': {'param1':'value1', 'param2':'value2'}
            | }

        :param ts_list: list of TS identifier
        :type ts_list: str or list

        :param with_type: boolean indicating the content to return
        :type with_type: bool

        :raises TypeError: if *ts_list* is neither a str nor a list
        """

        tdm = TemporalDataMgr()
        if with_type:
            return tdm.get_typed_meta_data(ts_list=ts_list)
        else:
            return tdm.get_meta_data(ts_list=ts_list)

    @staticmethod
    def update(tsuid, name, value, data_type=DTYPE.string, force_create=False):
        """
        Import a meta data into TemporalDataManager

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

        :return: execution status, True if import successful, False otherwise
        :rtype: bool

        :raises TypeError: if *tsuid* not a str
        :raises TypeError: if *name* not a str
        :raises TypeError: if *value* not a str or a number
        :raises TypeError: if *force_create* not a bool

        :raises ValueError: if *tsuid* is empty
        :raises ValueError: if *name* is empty
        :raises ValueError: if *value* is empty
        """

        tdm = TemporalDataMgr()
        return tdm.update_meta_data(tsuid=tsuid, name=name, value=value, data_type=data_type, force_create=force_create)


# noinspection PyMethodOverriding,PyAbstractClass
class IkatsDatasetMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Dataset management
    """

    def __init__(self, session):
        super(IkatsDatasetMgr).__init__(session)
        self.client = TemporalDataMgr(ikats_session=self.session)

    def save(self, ds):
        """
        Create a new data set composed of the *ts*

        :param ds: Dataset to create
        :type ds: Dataset

        :return: execution status (True if success, False otherwise)
        :rtype: bool

        :raises TypeError: if *ts* is not a list
        """

        if ds.name is None:
            raise ValueError("Dataset name is missing")
        if ds.ts is None or (type(ds.ts) is list and len(ds.ts) == 0):
            raise ValueError("No TS to save")

        self.client.dataset_create(
            name=ds.name,
            description=ds.description,
            ts=[x.tsuid for x in ds.ts])

    def read(self, name):
        """
        Reads the dataset information based on the dataset name.
        Retrieves description and list of Timeseries

        :param name: Name of the Dataset to read
        :type name: str

        :return: True if operation was successful, False if something wrong happens
        :rtype: bool

        :raise ValueError: if no dataset name provided
        """
        if name is None:
            raise ValueError("Dataset name shall be filled")

        result = TemporalDataMgr(ikats_session=self.session).dataset_read(name)

        ts = []
        if "ts_list" in result:
            ts = [Timeseries(tsuid=x['tsuid'], fid=x['funcId']) for x in result['ts_list']]

        description = ""
        if "description" in result:
            description = result['description']

        return Dataset(name=name, description=description, ts=ts)

    def delete(self, name, deep=False):
        """
        Remove data_set from base

        :param name: name of the data set to delete
        :type name: str

        :param deep: true to deeply remove dataset (tsuids and metadata erased)
        :type deep: boolean

        :return: True if operation is a success, False if error occurred
        :rtype: bool

        .. note::
           Removing an unknown data set results in a successful operation (server constraint)
           The only possible errors may come from server (HTTP status code 5xx)

        :raises TypeError: if *data_set* is not a str
        """

        return TemporalDataMgr(ikats_session=self.session).dataset_delete(name=name, deep=deep)

    @staticmethod
    def list():
        """
        Get the list of all data set and their corresponding description

        :return: key: data set, value: corresponding description : [{'name':name,'description':description}]
        :rtype: list of dict
        """

        tdm = TemporalDataMgr()
        return tdm.dataset_list()


class IkatsAPI:
    """
    Ikats resources API

    Common library of endpoints used by algorithms developers & contributors to access the data handled by Ikats.
    """

    def __init__(self, host="http://localhost", port="80", sc=None, name="IKATS_SESSION"):
        self.__session = IkatsSession(host=host, port=port, sc=sc, name=name)

        self.ts = IkatsTimeseriesMgr(session=self.__session)
        self.fid = IkatsFunctionalIdentifier(session=self.__session)
        self.md = IkatsMetadataMgr(session=self.__session)
        self.ds = IkatsDatasetMgr(session=self.__session)
        self.pd = IkatsProcessDataMgr(session=self.__session)
        self.table = IkatsTableMgr(session=self.__session)

    def __repr__(self):
        return "IKATS API"

# noinspection PyMethodOverriding,PyAbstractClass
from enum import Enum

from ikats.client import TDMClient
from ikats.manager.generic_ import IkatsGenericApiEndPoint


class DTYPE(Enum):
    """
    Enum used for Data types of Meta data
    """
    string = "string"
    date = "date"
    number = "number"
    complex = "complex"


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

        tdm = TDMClient()
        result = tdm.metadata_create(tsuid=tsuid, name=name, value=value, data_type=data_type,
                                     force_update=force_update)
        if not result:
            cls.LOGGER.error("Metadata '%s' couldn't be saved for TS %s", name, tsuid)
        return result

    def read(self, ts, name=None):
        """
        returns metadata information about the ts provided

        :param ts: Timeseries object or TSUID identifying the TS
        :param name: (optional) Name of the metadata to read (directly)

        :param ts: Timeseries or str
        :param name: str or None

        :return: the metadata value (if name provided) or the Metadata object corresponding to the ts provided
        :rtype: Metadata or object
        """
        # TODO, next step to do
        pass

    @staticmethod
    def old_read(ts_list, with_type=False):
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

        tdm = TDMClient()
        if with_type:
            return tdm.metadata_get_typed(ts_list=ts_list)
        else:
            return tdm.metadata_get(ts_list=ts_list)

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

        tdm = TDMClient()
        return tdm.metadata_update(tsuid=tsuid, name=name, value=value, data_type=data_type, force_create=force_create)

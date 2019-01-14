# noinspection PyMethodOverriding,PyAbstractClass

from ikats.client.tdm_client import TDMClient, DTYPE as C_TYPE
from ikats.lib import check_type
from ikats.manager.generic_ import IkatsGenericApiEndPoint
from ikats.objects.metadata_ import Metadata, DTYPE as O_TYPE


def _to_client_type(object_type):
    """
    Converts object DTYPE defined in Metadata object to the client DTYPE defined in TDM client

    :param object_type: value to convert
    :type object_type: O_TYPE

    :return: the converted value
    :rtype: C_TYPE
    """

    mapping_rule = {
        O_TYPE.STRING: C_TYPE.string,
        O_TYPE.COMPLEX: C_TYPE.complex,
        O_TYPE.NUMBER: C_TYPE.number,
        O_TYPE.DATE: C_TYPE.date,
    }

    if object_type not in mapping_rule:
        raise ValueError("Can't convert object DTYPE to client type: %s", object_type)
    return mapping_rule[object_type]


def _to_object_type(client_type):
    """
    Converts client DTYPE defined in TDM client to the object DTYPE defined in Metadata object

    :param client_type: value to convert
    :type client_type: C_TYPE

    :return: the converted value
    :rtype: O_TYPE
    """

    mapping_rule = {
        C_TYPE.string: O_TYPE.STRING,
        C_TYPE.complex: O_TYPE.COMPLEX,
        C_TYPE.number: O_TYPE.NUMBER,
        C_TYPE.date: O_TYPE.DATE,
    }

    if client_type not in mapping_rule:
        raise ValueError("Can't convert object Type to object type: %s", client_type)
    return mapping_rule[client_type]


class IkatsMetadataMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Metadata management
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = TDMClient(session=self.api.session)

    def create(self, tsuid, name, value, dtype=O_TYPE.STRING, force_update=False):
        """
        Import a meta data into TemporalDataManager

        :param tsuid: Functional Identifier of the TS
        :param name: Metadata name
        :param value: Value of the metadata
        :param dtype: data type of the meta data
        :param force_update: True to create the meta if not exists (default: False)

        :type tsuid: str
        :type name: str
        :type value: str or number
        :type dtype: O_TYPE
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

        result = self.client.metadata_create(tsuid=tsuid, name=name, value=value,
                                             data_type=_to_client_type(object_type=dtype),
                                             force_update=force_update)
        if not result:
            self.api.log.error("Metadata '%s' couldn't be saved for TS %s", name, tsuid)
        return result

    def fetch(self, metadata):
        """
        Fetch and return metadata information about the Metadata object provided

        The returned dict has the following format:
        {
          'md1':{'value':'value1', 'type': 'dtype'},
          'md2':{'value':'value2', 'type': 'dtype'}
        }

        :param metadata: Metadata object containing a valid tsuid
        :type metadata: Metadata

        :return: the object containing information about each metadata matching the TSUID.
        :rtype: dict
        """

        check_type(value=metadata, allowed_types=Metadata, raise_exception=True)
        result = self.client.metadata_get_typed(ts_list=[metadata.tsuid])[metadata.tsuid]

        # Converts DTYPE
        for md in result:
            result[md]["dtype"] = _to_object_type(C_TYPE[result[md]["dtype"]])

        return result

    def update(self, tsuid, name, value, data_type=O_TYPE.STRING, force_create=False):
        """
        Import a meta data into TemporalDataManager

        :param tsuid: TSUID of the TS
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

        return self.client.metadata_update(tsuid=tsuid, name=name, value=value,
                                           data_type=_to_client_type(object_type=data_type),
                                           force_create=force_create)

    def delete(self, tsuid, name, raise_exception=True):
        """
        Delete a metadata associated to a TSUID
        Returns True if everything is fine, False if an error happened

        :param tsuid:
        :param name: Name of the metadata
        :param raise_exception: Set to True to trigger exception

        :type tsuid: str
        :type name: str
        :type raise_exception: bool


        :return: The action status
        """
        return self.client.metadata_delete(tsuid=tsuid, name=name, raise_exception=raise_exception)

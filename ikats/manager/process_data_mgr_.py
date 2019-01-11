
# noinspection PyMethodOverriding,PyAbstractClass
from ikats.client import NTDMClient
from ikats.exceptions import IkatsException, IkatsNotFoundError
from ikats.manager.generic_ import IkatsGenericApiEndPoint


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

        ntdm = NTDMClient()
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
        ntdm = NTDMClient()
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
            ntdm = NTDMClient()
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
        ntdm = NTDMClient()
        ntdm.remove_data(process_id=process_id)

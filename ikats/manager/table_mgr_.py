

# noinspection PyMethodOverriding,PyAbstractClass
from ikats.client import TDMClient
from ikats.manager.generic_ import IkatsGenericApiEndPoint


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
        tdm = TDMClient()
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
        tdm = TDMClient()
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

        tdm = TDMClient()
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
        tdm = TDMClient()
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


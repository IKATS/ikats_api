# noinspection PyMethodOverriding,PyAbstractClass
from ikats.client import TDMClient
from ikats.exceptions import IkatsConflictError
from ikats.lib import check_type, check_is_valid_ds_name
from ikats.manager.generic_ import IkatsGenericApiEndPoint
from ikats.objects.dataset_ import Dataset
from ikats.objects.timeseries_ import Timeseries


class IkatsDatasetMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Dataset management
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = TDMClient(session=self.api.session)

    def new(self, name=None, desc=None, ts=None):
        """
        Create an empty local Dataset with optional name

        :param name: Dataset name to use
        :param desc: description of the dataset
        :param ts: list of Timeseries composing the dataset

        :type name: str
        :type desc: str
        :type ts: list of Timeseries

        :return: the Dataset object
        :rtype: Dataset

        :raises IkatsConflictError: if name already present in database
        """
        ds_list = self.client.dataset_list()
        if len([x for x in ds_list if x['name'] == name]) > 0:
            raise IkatsConflictError("The dataset name already exist in database, use 'get' method instead")
        return Dataset(api=self.api, name=name, desc=desc, ts=ts)

    def save(self, ds):
        """
        Create a new data set composed of the *ts*

        :param ds: Dataset to create
        :type ds: Dataset

        :return: execution status (True if success, False otherwise)
        :rtype: bool

        :raises TypeError: if *ts* is not a list
        """

        check_is_valid_ds_name(ds.name, raise_exception=True)
        if ds.ts is None or (type(ds.ts) is list and len(ds.ts) == 0):
            raise ValueError("No TS to save")

        self.client.dataset_create(
            name=ds.name,
            description=ds.desc,
            ts=[x.tsuid for x in ds.ts])

    def get(self, name):
        """
        Reads the dataset information in database
        Retrieves description and list of Timeseries

        :param name: Dataset name
        :type name: str

        :raise TypeError: if dataset name is malformed
        :raise IkatsNotFoundError: if dataset not found in database
        """
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)

        result = self.client.dataset_read(name)
        ts = [Timeseries(tsuid=x['tsuid'], fid=x['funcId'], api=self.api) for x in result.get('ts_list', [])]
        description = result.get("description", "")
        return Dataset(api=self.api, name=name, desc=description, ts=ts)

    def delete(self, name, deep=False):
        """
        Remove dataset from base

        :param name: Dataset name to delete
        :type name: str

        :param deep: true to deeply remove dataset (tsuid and metadata erased)
        :type deep: boolean

        :return: True if operation is a success, False if error occurred
        :rtype: bool

        .. note::
           Removing an unknown data set results in a successful operation (server constraint)
           The only possible errors may come from server (HTTP status code 5xx)

        :raises TypeError: if *name* is not a str
        :raises TypeError: if *deep* is not a bool
        """
        check_type(value=deep, allowed_types=[bool, None], var_name="deep", raise_exception=True)
        check_type(value=name, allowed_types=str, var_name="name", raise_exception=True)
        check_is_valid_ds_name(value=name, raise_exception=True)

        return self.client.dataset_delete(name=name, deep=deep)

    def list(self):
        """
        Get the list of all dataset

        :return: the list of Dataset objects
        :rtype: list of Dataset
        """

        return [Dataset(name=x["name"], desc=x["description"], api=self.api) for x in self.client.dataset_list()]

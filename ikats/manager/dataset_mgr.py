# noinspection PyMethodOverriding,PyAbstractClass
from ikats.client import TDMClient
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
            description=ds.description,
            ts=[x.tsuid for x in ds.ts])

    def read(self, ds):
        """
        Reads the dataset information in database and update the object
        Retrieves description and list of Timeseries

        :param ds: Dataset Object to read
        :type ds: Dataset

        :raise Type: if dataset malformed
        """
        check_type(value=ds, allowed_types=Dataset, var_name="ds", raise_exception=True)

        result = self.client.dataset_read(ds.name)
        ds.ts = [Timeseries(tsuid=x['tsuid'], fid=x['funcId'], api=self.api) for x in result.get('ts_list', [])]
        ds.description = result.get("description", "")

    def delete(self, ds, deep=False):
        """
        Remove data_set from base

        :param ds: Dataset to delete
        :type ds: str or Dataset

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
        check_type(value=ds, allowed_types=[Dataset, str], var_name="ds", raise_exception=True)
        if type(ds) == Dataset:
            ds = ds.name
        check_is_valid_ds_name(value=ds, raise_exception=True)
        check_type(value=deep, allowed_types=[bool, None], var_name="deep", raise_exception=True)

        return self.client.dataset_delete(name=ds, deep=deep)

    def list(self):
        """
        Get the list of all dataset

        :return: the list of Dataset objects
        :rtype: list of Dataset
        """

        return [Dataset(name=x["name"], description=x["description"], api=self.api) for x in
                self.client.dataset_list()]

#!/bin/python3
from ikats.objects.generic_ import IkatsObject
from ikats.objects.timeseries_ import Timeseries
from ikats.lib import check_type, check_is_valid_ds_name


class Dataset(IkatsObject):
    """
    Dataset class composed of information related to a single Dataset
    """

    def __init__(self, api, name=None, description=None, ts=None):
        """
        Initialization

        :param name: Name of the Dataset
        :param description: Description of the Dataset
        :param ts: List of Timeseries objects

        :type name: str or None
        :type description: str or None
        :type ts: list of Timeseries
        """

        # Internal variables initialization
        super().__init__(api)
        self.__name = None
        self.__description = None
        self.__ts = []
        self.__flag_ts_loaded = False

        # Initialization with provided parameters
        self.name = name
        self.description = description
        self.ts = ts

    @property
    def name(self):
        """
        Name of the dataset
        """
        return self.__name

    @name.setter
    def name(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="name", raise_exception=True)
        if value is not None:
            check_is_valid_ds_name(value=value, raise_exception=True)
            if self.__name != value:
                self.__flag_ts_loaded = False
                self.__name = value

    @property
    def description(self):
        """
        Description of the Dataset
        """
        return self.__description

    @description.setter
    def description(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="description", raise_exception=True)
        self.__description = value
        if value is None:
            self.__description = ""

    @property
    def ts(self):
        """
        List of Timeseries
        """
        # Lazy loading
        if not self.__flag_ts_loaded:
            if self.name is not None:
                self.__ts = self.api.ds.read(ds=self.name)
            self.__flag_ts_loaded = True
        return self.__ts

    @ts.setter
    def ts(self, value):
        check_type(value=value, allowed_types=[list, None], var_name="ts", raise_exception=True)
        if type(value) == list:
            for ts in value:
                if type(ts) not in [str, Timeseries]:
                    raise TypeError("Timeseries shall be a TSUID or Timeseries object")
            self.__ts = [x if type(x) == Timeseries else Timeseries(tsuid=x, api=self.api) for x in value]

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Dataset %s" % self.name

    def __len__(self):
        """
        :return: the number of Timeseries composing the dataset
        """
        return len(self.__ts)

    def __add__(self, other):
        """
        Creates a new Dataset composed of the list of Timeseries of both Original datasets

        :param other: other Dataset to add
        :type other: Dataset

        :return: The new Dataset
        :rtype: Dataset
        """

        # Builds the name of the resulting Dataset
        new_name = None
        if self.name is not None and other.name is not None:
            new_name = "{}-{}".format(self.name, other.name)

        return Dataset(api=self.api, name=new_name,
                       description=self.description,
                       ts=self.ts + other.ts)

    def add_ts(self, ts):
        """
        Append a Timeseries to this Dataset (but no save is performed)

        """
        if type(ts) == str:
            # Assuming this is a TSUID as a string
            ts_to_add = [Timeseries(tsuid=ts, api=self.api)]
        elif type(ts) == Timeseries:
            ts_to_add = [ts]
        elif type(ts) == list:
            ts_to_add = ts
        else:
            raise TypeError("Unknown type for Timeseries to add")
        self.ts = self.ts.extend(
            [x if type(x) == Timeseries else Timeseries(tsuid=x, api=self.api) for x in ts_to_add])

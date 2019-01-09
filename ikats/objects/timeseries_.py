#!/bin/python3
import copy

from ikats.objects.generic_ import IkatsObject
from ikats.objects.metadata_ import Metadata
from ikats.lib import check_type, check_is_fid_valid


class Timeseries(IkatsObject):
    def __init__(self, api, tsuid=None, fid=None, data=None):
        super().__init__(api)
        self.__md = {}
        self.__fid = None
        self.__data = []
        self.__flag_data_read = False

        self.tsuid = tsuid
        self.fid = fid
        self.data = data
        if tsuid is not None:
            self.md = Metadata(tsuid=tsuid, api=self.api)

    def __len__(self):
        return len(self.data)

    @property
    def data(self):
        if not self.__flag_data_read:
            self.api.ts.read(ts=self)
            self.__flag_data_read = True
        return self.__data

    @data.setter
    def data(self, value):
        if check_type(value, [list], "data", raise_exception=False):
            self.__data = value

    @property
    def metadata(self):
        if self.__md is None:
            self.__md = Metadata(tsuid=self.tsuid, api=self.api)
        return self.__md

    @metadata.setter
    def metadata(self, value):
        pass

    @property
    def tsuid(self):
        """
        Getter for tsuid
        """
        return self.__tsuid

    @tsuid.setter
    def tsuid(self, value):
        """
        Setter for tsuid
        """
        check_type(value=value, allowed_types=[str, None], var_name="tsuid", raise_exception=True)
        self.__tsuid = value

    @property
    def fid(self):
        """
        Getter for fid
        """
        return self.__fid

    @fid.setter
    def fid(self, value):
        """
        Setter for fid
        """
        if value is not None:
            check_is_fid_valid(fid=value, raise_exception=True)
            self.__fid = value

    def __str__(self):
        return self.tsuid

    def __repr__(self):
        return "Timeseries %s (%s)" % (self.fid, self.tsuid)

    def get_as_list(self):
        pass

    def get_as_spark_df(self, spark_context=None):
        if spark_context is None:
            raise ValueError("You shall provide Spark Context first")
        pass

    def get_as_np_array(self):
        pass

    def get_as_pd_array(self):
        pass

    def __add__(self, other):
        ts = copy.deepcopy(self)
        ts.data += other.data
        if ts.tsuid is None:
            ts.tsuid = other.tsuid
        if ts.fid is None:
            ts.fid = other.fid
        return ts

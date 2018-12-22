#!/bin/python3
from .session_ import IkatsSession
from .metadata_ import Metadata
from .utils import check_type


class Timeseries:
    def __init__(self, tsuid=None, fid=None, data=None, df=None, spark_context=None):
        self.__md = {}
        self.tsuid = tsuid
        self.fid = fid
        self.spark_context = spark_context
        self.data = data or df
        if data:
            self.dtype = "python"
        elif df:
            self.dtype = "dataframe"
        if tsuid is not None:
            self.md = Metadata(tsuid=tsuid)

    def __len__(self):
        if self.dtype == "dataframe":
            return self.data.count()
        elif self.dtype == "python":
            return len(self.data)

    @property
    def metadata(self):
        if self.__md is None:
            self.__md = Metadata(tsuid=self.tsuid)
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
        check_type(value=value, allowed_types=[str, None], var_name="fid", raise_exception=True)
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

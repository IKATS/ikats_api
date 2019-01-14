#!/bin/python3
import copy

from ikats.objects.generic_ import IkatsObject
from ikats.objects.metadata_ import Metadata
from ikats.lib import check_type, check_is_fid_valid


class Timeseries(IkatsObject):
    def __init__(self, api, tsuid=None, fid=None, data=None):
        super().__init__(api)
        self.__md = None
        self.__tsuid = None
        self.__fid = None
        self.__data = []
        self.__flag_data_read = False

        self.metadata = Metadata(api=api, tsuid=tsuid)
        self.tsuid = tsuid
        self.fid = fid
        self.data = data

    def __len__(self):
        return len(self.data)

    @property
    def data(self):
        if not self.__flag_data_read and self.__tsuid is not None:
            try:
                self.__data = self.api.ts.load(ts=self)
            except:
                # For fresh created Timeseries, there is no metadata so the `load` method can't be performed
                # since it needs the ikats_start_date and ikats_end_date to work properly
                # An exception is raised but nothing else to do
                pass
            self.__flag_data_read = True
        return self.__data

    @data.setter
    def data(self, value):
        if check_type(value, [list], "data", raise_exception=False):
            self.__data = value

    @property
    def metadata(self):
        return self.__md

    @metadata.setter
    def metadata(self, value):
        check_type(value=value, allowed_types=[Metadata], var_name="metadata", raise_exception=True)
        self.__md = value

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

        This setter shouldn't be used manually
        """
        check_type(value=value, allowed_types=[str, None], var_name="tsuid", raise_exception=True)
        if self.__tsuid != value:
            self.__tsuid = value
            # Update Metadata link
            self.metadata.tsuid = value

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

            # Try to get existing tsuid
            self.tsuid = self.api.ts.tsuid_from_fid(fid=self.fid, raise_exception=False)

    def __str__(self):
        if self.__tsuid is not None:
            return self.__tsuid
        return "<LocalTimeseries>"

    def __repr__(self):
        if self.__tsuid:
            return "Timeseries %s (%s)" % (self.fid, self.tsuid)
        else:
            return "<LocalTimeseries>"

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

    def load(self):
        """
        Loads data points from database and update local object and overwrite any existing points in local object
        """
        self.__data = self.api.ts.load(ts=self)

    def __add__(self, other):
        ts = copy.deepcopy(self)
        ts.data += other.data
        if ts.tsuid is None:
            ts.tsuid = other.tsuid
        if ts.fid is None:
            ts.fid = other.fid_from_tsuid
        return ts

    def save(self, generate_metadata=False):
        """
        Saves the Timeseries into database
        Generates the minimum required metadata if specified (used only when creating the TS for the first time

        :param generate_metadata:
        :type generate_metadata: bool

        :return:
        """
        self.api.ts.save(ts=self, generate_metadata=generate_metadata)

    def delete(self, raise_exception=True):
        """
        Deletes the Timeseries from database

        :param raise_exception: True to trigger the exception if they occurs
        :type raise_exception: bool

        :return: the status of the action
        """
        return self.api.ts.delete(ts=self.tsuid, raise_exception=raise_exception)

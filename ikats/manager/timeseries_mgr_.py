from ikats.client import TDMClient
from ikats.client.opentsdb_client import OpenTSDBClient
from ikats.client.tdm_client import DTYPE
from ikats.exceptions import IkatsConflictError, IkatsNotFoundError, IkatsException
from ikats.manager.generic_ import IkatsGenericApiEndPoint
from ikats.objects.timeseries_ import Timeseries
from ikats.lib import check_type, check_is_fid_valid, check_is_valid_epoch
import re

NON_INHERITABLE_PATTERN = re.compile("^qual(.)*|ikats(.)*|funcId")


class IkatsTimeseriesMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to TimeSeries management
    """

    def __init__(self, *args, **kwargs):
        super(IkatsTimeseriesMgr, self).__init__(*args, **kwargs)
        self.tsdb_client = OpenTSDBClient(session=self.api.session)
        self.tdm_client = TDMClient(session=self.api.session)

    def new(self, fid=None, *args, **kwargs):
        """
        Create an empty local Timeseries (if fid not provided)
        If fid is set, the identifier will be created in database (useful when importing data via spark for example)

        :param fid: Identifier to create (if provided)
        :type fid: str

        :return: the Timeseries object
        :rtype: Timeseries

        :raises IkatsConflictError: if FID already present in database
        """
        if fid is None:
            return Timeseries(api=self.api, *args, **kwargs)
        else:
            return self._create_ref(fid=fid)

    def get(self, fid=None, tsuid=None):
        """
        Returns an existing Timeseries object by providing either its FID or TSUID (only one shall be provided)

        :param fid: FID of the Timeseries
        :param tsuid: TSUID of the Timeseries

        :type fid: str
        :type tsuid: str

        :return: The Timeseries object
        :rtype: Timeseries

        :raises ValueError: if both fid and tsuid are set (or none of them
        """

        if fid is not None and tsuid is not None:
            raise ValueError("fid and tsuid are mutually exclusive")

        if fid is not None:
            tsuid = self.tsuid_from_fid(fid=fid, raise_exception=True)

        return Timeseries(api=self.api, tsuid=tsuid, fid=fid)

    def save(self, ts, parent=None, generate_metadata=True, raise_exception=True):
        """
        Import TS data points in database or update an existing TS with new points

        if generate_metadata is set or if no TSUID is present in ts object,
        the ikats_start_date, ikats_end_date and qual_nb_points will be
        overwritten by the first point date, last point date and number of points in ts.data

        :param ts: Timeseries object containing information about what to create
        :param parent: optional, default None: Timeseries object of inheritance parent
        :param generate_metadata: Generate metadata (set to False when doing partial import) (Default: True)
        :param raise_exception:

        :type ts: Timeseries
        :type parent: Timeseries
        :type generate_metadata: bool
        :type raise_exception: bool

        :return: an object containing several information about the import
        :rtype: dict

        """

        # Input checks
        check_type(ts, Timeseries, "ts", raise_exception=True)
        check_type(parent, [Timeseries, None], "parent", raise_exception=True)
        check_type(generate_metadata, bool, "generate_metadata", raise_exception=True)
        check_is_fid_valid(ts.fid, raise_exception=True)

        try:
            # First, we shall create the TSUID reference (if not provided)
            if ts.tsuid is None:
                ts.tsuid = self._create_ref(ts.fid).tsuid
                # If the TS is fresh, we force the creation of the metadata
                generate_metadata = True

            # Add points to this TSUID
            start_date, end_date, nb_points = self.tsdb_client.add_points(tsuid=ts.tsuid, data=ts.data)

            if generate_metadata:
                self.tdm_client.metadata_update(tsuid=ts.tsuid, name='ikats_start_date', value=start_date,
                                                data_type=DTYPE.date, force_create=True)

                self.tdm_client.metadata_update(tsuid=ts.tsuid, name='ikats_end_date', value=end_date,
                                                data_type=DTYPE.date, force_create=True)

                self.tdm_client.metadata_update(tsuid=ts.tsuid, name='qual_nb_points', value=nb_points,
                                                data_type=DTYPE.number, force_create=True)

            # Inherit from parent when it is defined
            if parent is not None:
                self.inherit(ts=ts, parent=parent)
        except IkatsException as exc:
            if raise_exception:
                raise
            else:
                return False
        return True

    def load(self, ts, sd=None, ed=None):
        """
        Retrieve the data corresponding to a Timeseries object
        Update it with data points if no sd or ed are specified

        .. note::
            if omitted, *sd* (start date) and *ed* (end date) will be retrieved from meta data for each TS
            if you want a fixed windowed range, set *sd* and *ed* manually (but be aware that the TS may be
            not completely gathered)

        :param ts: Timeseries object
        :param sd: optional starting date (timestamp in ms from epoch)
        :param ed: optional ending date (timestamp in ms from epoch)

        :type ts: Timeseries
        :type sd: int or None
        :type ed: int or None

        :returns: a list of ts data as numpy array
        :rtype: list of numpy array

        :raises TypeError: if *ts* is not a Timeseries object
        :raises TypeError: if *sd* is not an int
        :raises TypeError: if *ed* is not an int
        """

        check_type(value=ts, allowed_types=Timeseries, var_name="ts", raise_exception=True)
        check_type(value=sd, allowed_types=[int, None], var_name="sd", raise_exception=True)
        check_type(value=ed, allowed_types=[int, None], var_name="ed", raise_exception=True)

        if sd is None:
            sd = ts.metadata.get(name="ikats_start_date")
        check_is_valid_epoch(value=sd, raise_exception=True)

        if ed is None:
            ed = ts.metadata.get(name="ikats_end_date")
        check_is_valid_epoch(value=ed, raise_exception=True)

        data_points = self.tsdb_client.get_ts_by_tsuid(tsuid=ts.tsuid, sd=sd, ed=ed)

        # TODO transform data here

        # Return the points
        return data_points

    def delete(self, ts, raise_exception=True):
        """
        Delete the data corresponding to a ts and all associated metadata
        If timeseries belongs to a dataset it will not be removed

        Setting no_exception to True will prevent any exception to be raised.
        Useful to just try to delete a TS if it exists but have no specific action to perform in case of error

        :param ts: tsuid of the timeseries or Timeseries Object to remove
        :param raise_exception: True to raise Exceptions, False will just return boolean status

        :type ts: str or Timeseries
        :param raise_exception: bool

        :raises TypeError: if *tsuid* is not a str
        :raises IkatsNotFoundError: if *tsuid* is not found on server
        :raises IkatsConflictError: if *tsuid* belongs to -at least- one dataset
        :raises SystemError: if any other unhandled error occurred
        """

        check_type(value=ts, allowed_types=[str, Timeseries], var_name="ts", raise_exception=raise_exception)

        tsuid = ts

        if type(ts) == Timeseries:
            if ts.tsuid is not None:
                tsuid = ts.tsuid
            elif ts.fid is not None:
                try:
                    tsuid = self.tdm_client.get_tsuid_from_fid(fid=ts.fid)
                except IkatsNotFoundError:
                    if raise_exception:
                        raise
                    else:
                        return False

        return self.tdm_client.remove_ts(tsuid=tsuid, raise_exception=raise_exception)

    def inherit(self, ts, parent):
        """
        Make a time series inherit of parent's metadata according to a pattern (not all metadata inherited)

        :param ts: TS object in IKATS (which will inherit)
        :param parent: TS object in IKATS of inheritance parent

        :type ts: Timeseries
        :param parent: Timeseries
        """
        try:
            metadata = self.tdm_client.metadata_get([parent])[parent]
            for meta_name in metadata:
                if not NON_INHERITABLE_PATTERN.match(meta_name):
                    self.tdm_client.metadata_create(tsuid=ts.tsuid, name=meta_name, value=metadata[meta_name],
                                                    force_update=True)
        except(ValueError, TypeError, SystemError) as exception:
            self.api.session.log.warning(
                "Can't get metadata of parent TS (%s), nothing will be inherited; \nreason: %s", parent, exception)

    def list(self):
        """
        Get the list of all Timeseries in database

        :return: the list of Timeseries object
        :rtype: list
        """

        return [Timeseries(tsuid=x["tsuid"], fid=x["funcId"], api=self.api) for x in
                self.tdm_client.get_ts_list()]

    def find_from_meta(self, constraint=None):
        """
        From a meta data constraint provided in parameter, the method get a TS list matching these constraints

        Example of constraint:
            | {
            |     frequency: [1, 2],
            |     flight_phase: 8
            | }
        will find the TS having the following meta data:
            | (frequency == 1 OR frequency == 2)
            | AND
            | flight_phase == 8


        :returns: list of TSUID matching the constraints
        :rtype: dict

        :param constraint: constraint definition
        :type constraint: dict

        :raises TypeError: if *constraint* is not a dict
        """

        return self.tdm_client.get_ts_from_metadata(constraint=constraint)

    def fid_from_tsuid(self, tsuid, raise_exception=True):
        """
        Retrieve the functional ID associated to the tsuid param.

        :param tsuid: one tsuid value
        :param raise_exception: Allow to specify if the action shall assert if not found or not

        :type tsuid: str
        :type raise_exception: bool

        :return: retrieved functional identifier value
        :rtype: str

        :raises TypeError:  if tsuid is not a defined str
        :raises ValueError: no functional ID matching the tsuid
        :raises ServerError: http answer with status : 500 <= status < 600
        """
        try:
            return self.tdm_client.get_func_id_from_tsuid(tsuid=tsuid)
        except IkatsNotFoundError:
            if raise_exception:
                raise
            else:
                return None

    def tsuid_from_fid(self, fid, raise_exception=True):
        """
        Retrieve the TSUID associated to the functional ID param.

        :param fid: the functional Identifier
        :param raise_exception: Allow to specify if the action shall assert if not found or not

        :type fid: str
        :type raise_exception: bool

        :return: retrieved TSUID value or None if not found
        :rtype: str

        :raises TypeError:  if fid is not str
        :raises IkatsNotFoundError: no match
        """

        check_is_fid_valid(fid=fid)

        # Check if fid already associated to an existing tsuid
        try:
            return self.tdm_client.get_tsuid_from_fid(fid=fid)
        except IkatsNotFoundError:
            if raise_exception:
                raise
            else:
                return None

    def nb_points(self, tsuid):
        """
        return the effective imported number of points for a given tsuid

        :param tsuid: timeseries reference in db
        :type tsuid: str

        :return: the imported number of points
        :rtype: int

        :raises ValueError: if no TS with tsuid were found
        :raises SystemError: if openTSDB triggers an error
        """

        return self.tsdb_client.get_nb_points_of_tsuid(tsuid=tsuid)

    def _create_ref(self, fid):
        """
        Create a reference of timeseries in temporal data database and associate it to fid
        in temporal database for future use.
        Shall be used before create method in case of parallel creation of data (import data via spark for example)

        :param fid: Functional Identifier of the TS in Ikats
        :type fid: str

        :return: A prepared Timeseries object
        :rtype: Timeseries

        :raises IkatsConflictError: if FID already present in database
        """
        check_is_fid_valid(fid, raise_exception=True)
        try:
            # Check if fid already associated to an existing tsuid
            tsuid = self.tdm_client.get_tsuid_from_fid(fid=fid)
            # if fid already exist in database, raise a conflict exception
            raise IkatsConflictError("%s already associated to an existing tsuid: %s" % (fid, tsuid))

        except IkatsNotFoundError:
            # Creation of a new tsuid
            metric, tags = self.tsdb_client.gen_metric_tags()
            tsuid = self.tsdb_client.assign_metric(metric=metric, tags=tags)

            # finally importing tsuid/fid pair in non temporal database
            self.tdm_client.import_fid(tsuid=tsuid, fid=fid)

            return Timeseries(tsuid=tsuid, fid=fid, api=self.api)

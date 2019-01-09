
class IkatsObject:
    """
    Generic object for Dataset, Timeseries, Metadata, ...
    """

    def __init__(self, api):
        """

        :param api: IKATS session to use for connections
        :type api: IkatsAPI
        """
        self.__api = None
        self.api = api

    @property
    def api(self):
        return self.__api

    @api.setter
    def api(self, value):
        self.__api = value
        # if type(value) == IkatsAPI:
        #     self.__api = value
        # else:
        #     raise TypeError("Type of session shall be IkatsAPI, not %s" % (type(value)))

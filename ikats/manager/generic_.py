
class IkatsGenericApiEndPoint(object):
    """
    Abstract Ikats End Point class
    """

    def __init__(self, api):
        self.__api = None
        self.api = api

    @property
    def api(self):
        """
        api is a redirect to the Ikats API
        This allows High level object to interact with API to ease the User experience

        :return: the api
        :rtype: IkatsAPI
        """
        return self.__api

    @api.setter
    def api(self, value):
        self.__api = value

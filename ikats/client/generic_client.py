"""
Copyright 2018 CS Systèmes d'Information

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from enum import Enum

from ikats.exception import IkatsInputError, IkatsNotFoundError, IkatsClientError, IkatsServerError
from ikats.session_ import IkatsSession
from ikats.client import build_json_files, close_files
from ikats.lib import check_type


class RestClientResponse(object):
    """
    New wrapper of the result returned from module requests: it is returned by _send() method

    Available fields:
      - status_code
      - headers (new)
      - content_type (new)
      - url
      - json
      - text
      - content
      - raw

    You can use get_appropriate_content() in order to have the specified type of content
    """
    DEFAULT_JSON_INIT = "{}"

    def __init__(self, result):

        # The user ought to know which field to use
        #
        # => try to fill all fields (except self.json: lazily computed)
        # Reason: backward-compatibility (even if not optimal for memory)
        #
        # To be improved in next versions:
        # we ought to manage only one attr self.__result and use delegate design-pattern to
        # implement self properties text, raw, content (...) and associated getter functions
        # (see example with self.json)
        self.status_code = result.status_code
        self.headers = result.headers
        self.content_type = self.headers.get('content-type', None)
        self.url = result.url
        self.__json = None
        self.text = result.text
        self.raw = result.raw
        self.content = result.content
        self.__result = result

    @property
    def json(self):
        """
        The json getter: also available from self.json property.

        Note that there is a lazy computing of self.__json value, calling self.__result.json()
        made only once.

        :return: the effective json content deduced from self.__result. In case of error/empty body,
          RestClientResponse.DEFAULT_JSON_INIT is returned.
        """
        if self.__json is None:
            # default value backward-compatible with previous interface
            self.__json = RestClientResponse.DEFAULT_JSON_INIT
            try:
                self.__json = self.__result.json()
            except ValueError:
                # If the content is not json formatted, let the empty json fills the json field
                pass
        return self.__json

    def __str__(self):
        msg = "{} {}"
        return msg.format(self.status_code, self.url)

    @property
    def data(self):
        """
        :return: appropriate content according to the parsed content-type
        :rtype: object, str, or bytes

        :raises TypeError: error when there is no content specifically determined by content_type
        """
        if self.content_type == 'application/octet-stream':
            return self.content
        elif self.content_type == 'application/json':
            return self.json
        elif self.content_type == 'text/plain':
            return self.text
        else:
            raise TypeError("Failed to find appropriate content for content-type=%s", self.content_type)


class GenericClient(object):
    """
    Generic class to communicate using REST API
    """

    def __init__(self, session):
        self.__session = None
        self.session = session

    @property
    def session(self):
        """
        session contains necessary information to allow clients to connect to IKATS backend

        :return: the IKATS session
        : rtype: IkatsSession
        """
        return self.__session

    @session.setter
    def session(self, value):
        check_type(value, IkatsSession, "session", raise_exception=True)
        if value.rs is None:
            raise ValueError("Requests Session not set in provided IKATS session")
        self.__session = value

    class VERB(Enum):
        """
        Definition of possibilities for HTTP verb
        Only the following 4 are managed because they are the only allowed verbs for CRUD interface
        * CREATE -> POST
        * READ -> GET
        * UPDATE -> PUT
        * DELETE -> DELETE
        """
        POST = 0
        GET = 1
        PUT = 2
        DELETE = 3

    def send(self, root_url,
             verb=None,
             template="",
             uri_params=None,
             q_params=None,
             files=None,
             data=None,
             json_data=None,
             headers=None,
             timeout=300):
        """
        Generic call command that should not be called directly

        It performs the following actions:
        * checks the input type validity
        * calls the correct verb method from the library (get, post, put, delete)
        * formats the output (utf-8)
        * Handles the status_code from the server
        * decode the output
        * return the data

        :param root_url: Root part of the URL (domain, port and session root path)
        :param template: template to use for url building
        :param uri_params:  optional, default None: parameters applied to the template
        :param verb:  optional, default None: HTTP method to call
        :param q_params: optional, default None: list of query parameters
        :param files: optional, default None: files full path to attach to request
        :param data: optional, default None: data input consumed by request
            -note: when data is not None, json must be None
        :param json_data: optional, default None: json input consumed by request
            -note: when json is not None, data must be None
        :param headers: any headers to provide in request
        :param timeout: override the default timeout (300) before considering request as "lost"

        :type root_url: str
        :type template: str
        :type uri_params: dict
        :type verb: IkatsRest.VERB
        :type q_params: dict or None
        :type files: str or list or None
        :type data: object
        :type json_data: object
        :type headers: dict
        :type timeout: int

        :return: the response of the request
        :rtype: RestClientResponse

        :raises TypeError: if VERB is incorrect
        :raises TypeError: if FORMAT is incorrect
        :raises ValueError: if a parameter of uri_param contains spaces
        :raises ValueError: if there are unexpected argument values
        """
        check_type(value=verb, allowed_types=GenericClient.VERB, var_name="verb", raise_exception=True)

        if (data is not None) and (json_data is not None):
            raise ValueError("Integrity error: arguments data and json_data are mutually exclusive.")

        # Build the URL
        if uri_params is None:
            uri_params = {}
        url = "%s%s" % (root_url, template.format(**uri_params))

        # Converts file to 'requests' module format
        json_file = build_json_files(files)

        # Dispatch method
        try:
            if verb == GenericClient.VERB.POST:
                result = self.session.rs.post(url,
                                              data=data,
                                              json=json_data,
                                              files=json_file,
                                              params=q_params,
                                              timeout=timeout,
                                              headers=headers)
            elif verb == GenericClient.VERB.GET:
                result = self.session.rs.get(url,
                                             params=q_params,
                                             timeout=timeout,
                                             headers=headers)
            elif verb == GenericClient.VERB.PUT:
                result = self.session.rs.put(url,
                                             params=q_params,
                                             timeout=timeout,
                                             headers=headers)
            elif verb == GenericClient.VERB.DELETE:
                result = self.session.rs.delete(url,
                                                params=q_params,
                                                timeout=timeout,
                                                headers=headers)
            else:
                raise RuntimeError("Verb [%s] is unknown, shall be one defined by VERB Enumerate" % verb)

            # Format output encoding
            result.encoding = 'utf-8'

        except Exception as exception:
            self.session.log.error(exception)
            raise
        finally:
            # Close potential opened files
            close_files(json_file)

        return RestClientResponse(result)


def check_http_code(response):
    """
    Inspect http response and throws error if needed

    :param response: http response handled
    :type response: RestClientResponse

    :raises IkatsInputError: if status_code 400 (bad request)
    :raises IkatsNotFoundError: mismatched result: http status_code 404:  not found
    :raises IkatsClientError: unexpected client error
    :raises IkatsServerError: unexpected server error
    """

    if str(response.status_code)[0] == '2':
        # HTTP_CODE == 2XX
        return
    is_400(response, "Invalid parameters sent")
    is_404(response, "No Results")
    is_4xx(response, "Unexpected client error: {code}")
    is_5xx(response, "Unexpected server error: {code}")


def is_400(response, msg):
    """
    Detects a 400 HTTP error code.

    :param response: response of the request
    :param msg: msg to display in case of match

    :type response: RestClientResponse
    :type msg: str
    """
    if response.status_code == 400:
        raise IkatsInputError(msg)


def is_404(response, msg):
    """
    Detects a 404 HTTP error code.

    :param response: response of the request
    :param msg: msg to display in case of match

    :type response: RestClientResponse
    :type msg: str
    """
    if response.status_code == 404:
        raise IkatsNotFoundError(msg)


def is_4xx(response, msg):
    """
    Detects a 4XX HTTP error code.
    Use "{code}" in msg to use the obtained HTTP code

    :param response: response of the request
    :param msg: msg to display in case of match

    :type response: RestClientResponse
    :type msg: str
    """
    if str(response.status_code)[0] == '4':
        if "{code}" in msg:
            msg = msg.format(**{"code": response.status_code})
        raise IkatsClientError(msg)


def is_5xx(response, msg):
    """
    Detects a 5XX HTTP error code.
    Use "{code}" in msg to use the obtained HTTP code

    :param response:
    :param msg:

    """
    if str(response.status_code)[0] == '5':
        if "{code}" in msg:
            msg = msg.format(**{"code": response.status_code})
        raise IkatsServerError(msg)

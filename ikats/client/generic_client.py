# -*- coding: utf-8 -*-
"""
Copyright 2019 CS Systèmes d'Information

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
import mimetypes
from enum import Enum

from ikats.exceptions import (IkatsClientError, IkatsConflictError,
                              IkatsInputError, IkatsNotFoundError,
                              IkatsServerError)
from ikats.lib import check_type
from ikats.objects.session_ import IkatsSession


def close_files(json):
    """
    Closes the files opened with build_json_files method

    :param json: item built using build_json_files method
    :type json: dict or list
    """

    if isinstance(json, dict):
        # One file to handle
        json['file'].close()
    elif isinstance(json, list):
        # Multiple files
        for i in json:
            json[i][1][1].close()


def build_json_files(files):
    """
    Build the json files format to provide when sending files in a request

    :param files: file or list of files to use for building json format
    :type files: str OR list

    :returns: the json to pass to request object
    :rtype: dict

    Single file return format
        | files = {'file': ('report.xls', open('report.xls', 'rb'), 'application/vnd.ms-excel', {'Expires': '0'})}

    Multiple files return format
        | files = [('images', ('foo.png', open('foo.png', 'rb'), 'image/png')),
        |          ('images', ('bar.png', open('bar.png', 'rb'), 'image/png'))]


    :raises TypeError: if file is not found
    :raises ValueError: if MIME hasn't been found for the file
    """

    if isinstance(files, str):

        # Only one file is provided
        working_file = files

        # Defines MIME type corresponding to file extension
        mime = mimetypes.guess_type(working_file)[0]
        if mime is None:
            raise ValueError("MIME type not found for file %s" % working_file)

        # Build results
        # results = {'file': (f, open(f, 'rb'), mime, {'Expires': '0'})}
        return {'file': open(working_file, 'rb')}

    if isinstance(files, list):
        # Multiple files are provided
        results = []
        for working_file in files:
            # Defines MIME type corresponding to file extension
            mime = mimetypes.guess_type(working_file)[0]
            if mime is None:
                raise ValueError("MIME type not found for file %f" % working_file)
            # Build result
            results.append(('file', (working_file, open(working_file, 'rb'), mime)))
        return results

    if files is None:
        # No file is provided -> No treatment
        return None

    # Handling errors
    raise TypeError("Files must be provided as str or list (got %s)" % type(files))


class RestClientResponse:
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

        :returns: the effective json content deduced from self.__result. In case of error/empty body,
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
        :returns: appropriate content according to the parsed content-type
        :rtype: object, str, or bytes

        :raises TypeError: error when there is no content specifically determined by content_type
        """

        if 'application/octet-stream' in self.content_type:
            return self.content
        if 'application/json' in self.content_type:
            return self.json
        if 'text/plain' in self.content_type:
            return self.text
        raise TypeError("Failed to find appropriate content for content-type=%s" % self.content_type)


class GenericClient:
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

        :returns: the IKATS session
        :rtype: IkatsSession
        """
        return self.__session

    @session.setter
    def session(self, value):
        check_type(value=value, allowed_types=IkatsSession, var_name="session", raise_exception=True)
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
             timeout=300,
             session=None):
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
        :param session: allow to use a specific session (instead of the provided one)
                        Useful when needing to write multiple times in a short time

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
        :type session: requests.session

        :returns: the response of the request
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

        # Use custom session if provided
        session_to_use = self.session.rs
        if session is not None:
            session_to_use = session

        # Dispatch method
        try:
            if verb == GenericClient.VERB.POST:
                result = session_to_use.post(url, data=data,
                                             json=json_data,
                                             files=json_file,
                                             params=q_params,
                                             timeout=timeout,
                                             headers=headers)
            elif verb == GenericClient.VERB.GET:
                result = session_to_use.get(url, params=q_params,
                                            timeout=timeout,
                                            headers=headers)
            elif verb == GenericClient.VERB.PUT:
                result = session_to_use.put(url, params=q_params,
                                            timeout=timeout,
                                            headers=headers)
            elif verb == GenericClient.VERB.DELETE:
                result = session_to_use.delete(url, params=q_params,
                                               timeout=timeout,
                                               headers=headers)
            else:
                raise ValueError("Verb [%s] is unknown, shall be one defined by VERB Enumerate" % verb)

            # Format output encoding
            result.encoding = 'utf-8'

        except Exception as ex:
            raise IkatsServerError("IKATS not reachable", ex)
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


def is_409(response, msg):
    """
    Detects a 409 HTTP error code.

    :param response: response of the request
    :param msg: msg to display in case of match

    :type response: RestClientResponse
    :type msg: str
    """
    if response.status_code == 409:
        raise IkatsConflictError(msg)


def is_4xx(response, msg):
    """
    Detects a 4XX HTTP error code.
    Use "{code}" in msg to use the obtained HTTP code

    :param response: response of the request
    :param msg: msg to display in case of match

    :type response: RestClientResponse
    :type msg: str
    """
    if str(response.status_code).startswith('4'):
        if "{code}" in msg:
            msg = msg.format(**{"code": response.status_code})
        raise IkatsClientError(msg)

# TODO : Doc
def is_5xx(response, msg):
    """
    Detects a 5XX HTTP error code.
    Use "{code}" in msg to use the obtained HTTP code

    :param response:
    :param msg:

    :type response:
    :type msg:
    """
    if str(response.status_code).startswith('5'):
        if "{code}" in msg:
            msg = msg.format(**{"code": response.status_code})
        raise IkatsServerError(msg)

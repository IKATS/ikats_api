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
import os

from ikats.client import RestClient


class NonTemporalDataMgr(RestClient):
    """
    Class managing all non temporal data transactions.
    Used to store/retrieve algorithms results that are not timeseries
    """

    def __init__(self, *args, **kwargs):
        super(NonTemporalDataMgr, self).__init__(*args, **kwargs)

    def add_data(self, data, process_id, data_type=None, name=None):
        """
        Request to add a new data

        .. deprecated:: Story 156651
            data_type shouldn't be used anymore. Leave it to None (default value)

        :param data_type: data format: "JSON","CSV"
        :type data_type: str or None
        :param process_id: process id to store information to
        :param data: data to store
        :param name: name of the json to store (only used for json)
        :return: execution status_code: dict with entries:
          - 'status_code': True with success (HTTP Code 200 returned); False otherwise
          - 'id': id of created process_data

        :raise ValueError: if data_type is not handled
        :raise TypeError: data content type is not handled
        """

        # Default values
        filename = None
        json_data = None
        headers = None

        if data_type == "JSON":
            template = "add_process_data_json"
            if type(data) is str:
                if name is None:
                    name = "JSON_result"

                post_data = {
                    'size': len(data),
                    'name': name,
                    'json': data,
                }
            else:
                self.logger.error("'data' must be a string (got: %s %s)", type(data), data)
                raise TypeError("'data' must be a string (got: %s %s)" % (type(data), data))

        elif data_type == "CSV":
            template = 'add_process_data'

            if type(data) is str:
                if not os.path.isfile(data):
                    self.logger.error("The file [%s] doesn't exists", data)
                    raise FileNotFoundError("The file [%s] doesn't exists" % data)
                # The data is already a file formatted to correct format
                filename = data

                post_data = {
                    "fileType": data_type,
                    "fileSize": os.path.getsize(filename),
                }
            else:
                self.logger.error("'data' must be a valid file path (got: %s %s)", type(data), data)
                raise TypeError("'data' must be a valid file path (got: %s %s)" % (type(data), data))

        elif data_type is None:
            # Newer way to store any blob information.
            post_data = data
            template = 'add_process_data_any'
        else:
            self.logger.error("Unhandled data_type value (got: %s %s)", type(data_type), data_type)
            raise ValueError("Unhandled data_type value (got: %s %s)" % (type(data_type), data_type))

        # List of items to be replaced by in the template
        uri_params = {
            'process_id': process_id,
            'name': name,
        }

        result = {'status_code': False}

        response = _send(
            verb=RestClient.VERB.POST,
            template=template,
            data=post_data,
            json_data=json_data,
            uri_params=uri_params,
            files=filename,
            headers=headers)

        if response.status == 200:
            result['status_code'] = True

        result['id'] = response.text
        return result

    def download_data(self, data_id):
        """
        Request to find a data

        :param data_id:
        :return: data + execution status_code
        """
        # Checks inputs
        if type(data_id) is not str:
            self.logger.error("data_id must be a string (got %s)", type(data_id))
            raise TypeError("data_id must be a string (got %s)" % type(data_id))

        # List of items to be replaced by in the template
        uri_params = {
            'id': data_id
        }

        response = _send(
            verb=RestClient.VERB.GET,
            template='download_process_data',
            uri_params=uri_params)
        return response

    def remove_data(self, process_id):
        """
        Remove process data from base for a processExecId

        Corresponding web app resource operation: **removeprocess_data**

        :param process_id: name of the processId to delete
        :type process_id: str

        :return: True if operation is a success, False if error occurred
        :rtype: bool

        .. note::
           Removing an unknown process data results in a successful operation (server constraint)
           The only possible errors may come from server (HTTP status_code code 5xx)

        :raises TypeError: if *data_set* is not a str
        """
        # Checks inputs
        if type(process_id) is not str:
            self.logger.error("process_id must be a string (got %s)", type(process_id))
            raise TypeError("process_id must be a string (got %s)" % type(process_id))

        # List of items to be replaced by in the template
        uri_params = {
            'id': process_id
        }

        response = _send(
            verb=RestClient.VERB.DELETE,
            template='remove_process_data',
            uri_params=uri_params)

        if 200 <= response.status < 300:
            return True
        return False

    def get_data(self, process_id):
        """"
        Get all process data from base for a processExecId

        Corresponding web app resource operation: **getprocess_data**

        :param process_id: processId to delete
        :type process_id: str

        :return: True if operation is a success, False if error occurred
        :rtype: bool

        .. note::
           Removing an unknown data set results in a successful operation (server constraint)
           The only possible errors may come from server (HTTP status_code code 5xx)

        :raises TypeError: if *data_set* is not a str
        """
        # Checks inputs
        if type(process_id) is not str:
            self.logger.error("process_id must be a string (got %s)", type(process_id))
            raise TypeError("process_id must be a string (got %s)" % type(process_id))

        # List of items to be replaced by in the template
        uri_params = {
            'id': process_id
        }

        response = _send(
            verb=RestClient.VERB.GET,
            template='get_process_data',
            uri_params=uri_params)

        return response.json

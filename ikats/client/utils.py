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
import mimetypes

# List of templates used to build URL.
#
# * Key corresponds to the web app method to use
# * Value contains
#    * the pattern of the url to connect to
TEMPLATES = {

    # NON TEMPORAL DATA MANAGER
    'add_process_data': {
        'pattern': '/processdata/{process_id}',
    },
    'add_process_data_json': {
        'pattern': '/processdata/{process_id}/JSON',
    },
    'add_process_data_any': {
        'pattern': '/processdata?name={name}&processId={process_id}',
    },
    'get_process_data': {
        'pattern': '/processdata/{id}',
    },
    'remove_process_data': {
        'pattern': '/processdata/{id}',
    },
    'download_process_data': {
        'pattern': '/processdata/id/download/{id}',
    },

    # TABLES
    'list_tables': {
        'pattern': '/table'
    },
    'create_table': {
        'pattern': '/table'
    },
    'read_table': {
        'pattern': '/table/{name}'
    },
    'delete_table': {
        'pattern': '/table/{name}'
    },

}


def close_files(json):
    """
    Closes the files opened with build_json_files method
    :param json: item built using build_json_files method
    :type json: dict or list
    """

    if type(json) is dict:
        # One file to handle
        json['file'].close()
    elif type(json) is list:
        # Multiple files
        for i in json:
            json[i][1][1].close()


def build_json_files(files):
    """
    Build the json files format to provide when sending files in a request

    :param files: file or list of files to use for building json format
    :type files: str OR list

    :return: the json to pass to request object
    :rtype: dict

    Single file return format
        | files = {'file': ('report.xls', open('report.xls', 'rb'), 'application/vnd.ms-excel', {'Expires': '0'})}

    Multiple files return format
        | files = [('images', ('foo.png', open('foo.png', 'rb'), 'image/png')),
        |          ('images', ('bar.png', open('bar.png', 'rb'), 'image/png'))]


    :raises TypeError: if file is not found
    :raises ValueError: if MIME hasn't been found for the file
    """

    if type(files) is str:

        # Only one file is provided
        working_file = files

        # Defines MIME type corresponding to file extension
        mime = mimetypes.guess_type(working_file)[0]
        if mime is None:
            raise ValueError("MIME type not found for file %s" % working_file)

        # Build results
        # results = {'file': (f, open(f, 'rb'), mime, {'Expires': '0'})}
        return {'file': open(working_file, 'rb')}

    elif type(files) is list:
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

    elif files is None:
        # No file is provided -> No treatment
        return None

    else:
        # Handling errors
        raise TypeError("Files must be provided as str or list (got %s)" % type(files))

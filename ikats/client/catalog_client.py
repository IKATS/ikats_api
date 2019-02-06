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

from ikats.client.generic_client import GenericClient, is_4xx, is_5xx, is_404
# List of templates used to build URL.
#
# * Key corresponds to the web app method to use
# * Value contains
#    * the pattern of the url to connect to
from ikats.exceptions import IkatsException

TEMPLATES = {
    'implem_list': '/implementations',
    'implem': '/implementations/{name}'
}


class CatalogClient(GenericClient):
    """
    Catalog client used to connect to Python catalog backend
    """

    def __init__(self, *args, **kwargs):
        super(CatalogClient, self).__init__(*args, **kwargs)
        self.root_url = "/ikats/algo/catalogue"

    def get_implementation_list(self):
        """
        Get the list of all implementations from database

        :returns: the list of implementations
        :rtype: list
        """
        response = self.send(root_url=self.session.catalog_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['implem_list'])

        if response.status_code == 200:
            return response.json
        if response.status_code == 404:
            return []
        is_4xx(response, "Unexpected client error : {code}")
        is_5xx(response, "Unexpected server error : {code}")
        return []

    def get_implementation(self, name):
        """
        Get the implementations matching the name from database

        :param name: identifier of the implementation to get
        :type name: str

        :returns: the dict of implementation
        :rtype: dict
        """
        response = self.send(root_url=self.session.catalog_url + self.root_url,
                             verb=GenericClient.VERB.GET,
                             template=TEMPLATES['implem'],
                             uri_params={
                                 "name": name
                             })

        if response.status_code == 200:
            return response.json

        is_404(response, "No implementation found matching " + name)
        is_4xx(response, "Unexpected client error : {code}")
        is_5xx(response, "Unexpected server error : {code}")
        raise IkatsException("Something wrong happened")

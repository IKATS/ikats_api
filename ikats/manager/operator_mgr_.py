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
from ikats.client.catalog_client import CatalogClient
from ikats.client.catalog_stub import CatalogStub
from ikats.client.datamodel_client import DatamodelClient
from ikats.client.datamodel_stub import DatamodelStub
from ikats.manager.generic_mgr_ import IkatsGenericApiEndPoint
from ikats.objects import InOutParam, Operator


def merge_json_to_op(op, json):
    """
    Map the json keys to operator members
    Updates the operator

    :param op: operator to update
    :param json: data containing information to update

    :type op: Operator
    :type json: dict
    """

    op.op_id = json.get("id", op.op_id)
    op.desc = json.get("description", op.desc)
    op.label = json.get("label", op.label)
    op.family = json.get("family", op.family)

    op.inputs = []
    for item in json.get("inputs", []):
        op.inputs.append(InOutParam(json_data=item, api=op.api))

    op.parameters = []
    for item in json.get("parameters", []):
        op.parameters.append(InOutParam(json_data=item, api=op.api))

    op.outputs = []
    for item in json.get("outputs", []):
        op.outputs.append(InOutParam(json_data=item, api=op.api))


class IkatsOperatorMgr(IkatsGenericApiEndPoint):
    """
    Ikats EndPoint specific to Operators management
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dm_client = DatamodelClient(session=self.api.session)
        if self.api.emulate:
            self.dm_client = DatamodelStub(session=self.api.session)
            self.cat_client = CatalogStub(session=self.api.session)
        else:
            self.dm_client = DatamodelClient(session=self.api.session)
            self.cat_client = CatalogClient(session=self.api.session)

    def list(self):
        """
        Get the list of operators

        :returns: the list of all available operators
        :rtype: list of Operators
        """
        results = []
        raw_op_list = self.cat_client.get_implementation_list()
        for item in raw_op_list:
            op = Operator(api=self.api, name=item.get("name", None))
            merge_json_to_op(op, item)
            results.append(op)
        return results

    def get(self, name):
        """
        Get a specific operator identified by its name

        :param name: Identifier of the operator (unique name)
        :type name: str

        :returns: the operator
        :rtype: Operator

        :raises IkatsNotFoundError: if no match
        """
        op = Operator(api=self.api, name=name)
        raw_op = self.cat_client.get_implementation(name=name)
        merge_json_to_op(op, raw_op)
        return op

    def run(self, op):
        """
        Runs the configured operator

        :param op: Operator containing necessary information to be run
        :type op: Operator

        :returns: Operator with running status elements
        :rtype: OpRunner
        """
        raise NotImplementedError("No yet implemented")

    def results(self, pid):
        """
        Returns the results of the process ID specified

        :param pid: the ProcessId
        :type pid: str or int

        :returns: list of the results
        :rtype: list
        """
        return self.dm_client.pid_results(pid=pid)

    def result(self, rid):
        """
        Returns the results of the process ID specified

        :param rid: the ResultId
        :type rid: str or int

        :returns: specific result (type depends on the corresponding output type)
        :rtype: object
        """
        return self.dm_client.rid_get(rid=rid)

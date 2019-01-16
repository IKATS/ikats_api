from ikats.client import TDMClient
from ikats.client.catalog_client import CatalogClient
from ikats.manager.generic_ import IkatsGenericApiEndPoint
from ikats.objects.operator_ import Operator, InOutParam


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
        self.cat_client = CatalogClient(session=self.api.session)
        self.tdm_client = TDMClient(session=self.api.session)

    def list(self):
        """
        Get the list of operators

        :return: the list of all available operators
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

        :return: the operator
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

        :return: Operator with running status elements
        :rtype: OpRunner
        """

    def results(self, pid):
        """
        Returns the results of the process ID specified

        :param pid: the ProcessId
        :type pid: str or int

        :return: list of the results
        :rtype: list
        """
        return self.tdm_client.pid_results(pid=pid)

    def result(self, rid):
        """
        Returns the results of the process ID specified

        :param rid: the ResultId
        :type rid: str or int

        :return: specific result (type depends on the output type)
        :rtype: object
        """
        return self.tdm_client.pid_result(rid=rid)

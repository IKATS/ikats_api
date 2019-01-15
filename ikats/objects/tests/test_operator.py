from unittest import TestCase

from ikats import IkatsAPI
from ikats.exceptions import IkatsConflictError
from ikats.objects.operator_ import InOutParam


class TestOperator(TestCase):
    def test_nominal(self):
        """
        Get an Operator instance
        """
        api = IkatsAPI()

        op = api.op.get(name="slope_spark")
        self.assertIsNotNone(op.op_id)
        self.assertIsNotNone(op.desc)
        self.assertIsNotNone(op.label)
        self.assertEqual(1, len(op.inputs))
        self.assertEqual(InOutParam, type(op.inputs[0]))
        self.assertEqual(0, len(op.parameters))
        self.assertEqual(1, len(op.outputs))
        self.assertEqual(InOutParam, type(op.outputs[0]))


class Operator:
    def __init__(self, name, spark_session):
        self.name = name
        self.spark_session = spark_session
        self.pid = None

    def run(self, *args, **kwargs):
        pass

    def status(self):
        pass

import os

from datawatch.alerts import Alerter

class EmailAlerter(Alerter):
    name = 'email'

    def __init__(self, *args, **kwargs):
        pass

    def get_failed_test_message(self, table_config):
        pass

    def get_datatest_message(self, table_config, data_test):
        pass

    def alert(self, table_config, data_test):
        if data_test == None:
            message = self.get_failed_test_message(table_config)
        else:
            message = self.get_datatest_message(table_config, data_test)

        ## TODO: send email

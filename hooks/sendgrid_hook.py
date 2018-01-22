from airflow.hooks.http_hook import HttpHook
import json
import logging


class SendgridHook(HttpHook):
    def __init__(
            self,
            method='GET',
            http_conn_id='http_default',
            *args,
            **kwargs):
        self._args = args
        self._kwargs = kwargs

        self.connection = self.get_connection(http_conn_id)
        self.extras = self.connection.extra_dejson
        self.api_key = self.extras['api_key']
        super().__init__(method, http_conn_id)

    def run(self, endpoint, data=None, headers=None, extra_options=None, extra_args={}):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.api_key)
        }

        return super().run(endpoint, data, headers, extra_options)

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

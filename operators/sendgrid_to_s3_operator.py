import logging
import json
import collections

import urllib.request
import tarfile

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults

from sendgrid_plugin.hooks.sendgrid_hook import SendgridHook
from tempfile import NamedTemporaryFile

mappings = {
    'blocks': 'suppression/blocks',
    'bounces': 'suppression/bounces',
    'invalid_emails': 'suppression/invalid_emails',
    'spam_reports': 'suppression/spam_reports',
    'stats': 'stats'
}


class EndpointNotSupported(Exception):
    def __init__(self) -> None:
        super().__init__("Specified endpoint not currently supported.")


class SendgridToS3Operator(BaseOperator, SkipMixin):
    """
    Make a query against Sendgrid and write the resulting data to s3
    """
    template_field = ('s3_key', )

    @apply_defaults
    def __init__(
        self,
        sendgrid_conn_id,
        sendgrid_endpoint,
        sendgrid_args=None,
        s3_conn_id=None,
        s3_key=None,
        s3_bucket=None,
        *args,
        **kwargs
    ):
        """ 
        Initialize the operator
        :param sendgrid_conn_id:        name of the Airflow connection that has
                                        your Sendgrid api key
        :param sendgrid_endpoint:       name of the Sendgrid endpoint we are
                                        fetching data from. Implemented for 
                                            - blocks
                                            - bounces
                                            - spam_reports
                                            - stats
                                            - invalid_emails
        :param sendgrid_args           *(optional)* dictionary with extra Sendgrid
                                        arguments. For stats endpoint you should set
                                        start_date (default to 2017-01-01)
        :param s3_conn_id:              name of the Airflow connection that has
                                        your Amazon S3 conection params
        :param s3_bucket:               name of the destination S3 bucket
        :param s3_key:                  name of the destination file from bucket
        """

        super().__init__(*args, **kwargs)

        if sendgrid_endpoint not in mappings:
            raise EndpointNotSupported()

        self.sendgrid_conn_id = sendgrid_conn_id
        self.sendgrid_endpoint = sendgrid_endpoint

        if sendgrid_endpoint == 'stats':
            if sendgrid_args is None:
                sendgrid_args = {}
            sendgrid_args['start_date'] = '2017-01-01'

        self.sendgrid_args = sendgrid_args

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def get_all(self):
        extra_args = self.sendgrid_args if self.sendgrid_args is not None else {}
        extra_args['limit'] = 10 
        extra_args['offset'] = 0

        response = self.hook.run(
            mappings[self.sendgrid_endpoint], data=extra_args)
        results = response.json()

        while len(response.json()) == extra_args['limit']:
            extra_args['offset'] += extra_args['limit']
            response = self.hook.run(response.headers['link'])
            results.extend(response.json())

        return results

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Sendgrid endpoint
        and write it to a file.
        """
        logging.info("Prepping to gather data from Sendgrid")
        self.hook = SendgridHook(
            http_conn_id=self.sendgrid_conn_id
        )


        logging.info(
            "Making request for"
            " {0} object".format(self.sendgrid_endpoint)
        )

        results = self.get_all()

        if len(results) == 0 or results is None:
            logging.info("No records pulled from Sendgrid.")
            downstream_tasks = context['task'].get_flat_relatives(
                upstream=False)
            logging.info('Skipping downstream tasks...')
            logging.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)
            return True

        else:
            # Write the results to a temporary file and save that file to s3.
            with NamedTemporaryFile("w") as tmp:
                for result in results:
                    tmp.write(json.dumps(result) + '\n')

                tmp.flush()

                dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True

                )
                dest_s3.connection.close()
                tmp.close()

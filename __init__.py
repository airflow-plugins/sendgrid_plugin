from airflow.plugins_manager import AirflowPlugin

from sendgrid_plugin.operators.sendgrid_to_s3_operator import SendgridToS3Operator
from sendgrid_plugin.hooks.sendgrid_hook import SendgridHook


class sendgrid_plugin(AirflowPlugin):
    name = "sendgrid_plugin"
    operators = [SendgridToS3Operator]
    hooks = [SendgridToS3Operator]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

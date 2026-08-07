"""
### adsales_digital_youtube

**DAG Description**: This DAG triggers AdSales Digital YouTube dbt jobs.

**Config**: `adsales_digital_youtube.py`

**Version History**

| Version | Date        | Ticket Details | By                                |
| ------- | ----------- | -------------- | --------------------------------- |
| V1      | 10 Jun 2025 | BIADS-16074    | [Alima Afrose Shahul Hameed](mailto:alimaafroseshahul.hameed@fox.com) |

#### Alerts
- **Slack**: #adsales-slack-alert, #mc_notifications_adsales  
- **Email**: dataproductsupport@fox.com, fox.ai@fox.com
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from include.adsales.scripts.common.dbt_cloud_utils import DBTCloudUtils
from dags.config.adsales.adsales_digital_youtube import adsales_digital_youtube_config
from include.adsales.scripts.common.airflow_common import local_tz, default_args
import logging, os
import subprocess
import sys
from include.commons.utils.config_utils import DagConfig
from include.adsales.scripts.common.NotificationManager import NotificationManager
from airflow_mcd.callbacks import mcd_callbacks

logger = logging.getLogger(__name__)
env = 'dev' if Variable.get('env', default_var='dev') == 'local' else Variable.get('env', default_var='dev')
config = adsales_digital_youtube_config[env]
logger.info(f"Running in {env} environment.")

dag_default_config = config["dag_default_config"]
# Notification Manager
message = NotificationManager(
    config=DagConfig.validate_and_build_config(config_dict=adsales_digital_youtube_config, env=env))

default_args = {
    **dag_default_config,
    "on_failure_callback": [message.on_failure_callback, mcd_callbacks.mcd_task_failure_callback],
    # Slack + Email + MC Task Callbacks
    "on_execute_callback": [mcd_callbacks.mcd_task_execute_callback],
    "on_success_callback": [mcd_callbacks.mcd_task_success_callback],
    "on_retry_callback": [mcd_callbacks.mcd_task_retry_callback],
}


def build_python_cmd(script_path, *args):
    cmd = {"script_path": script_path}
    if args:
        cmd["script_args"] = list(args)
    return cmd


# def run_python_script(script_path, script_args=None, **kwargs):
#     cmd = [sys.executable, script_path]
#     if script_args:
#         cmd.extend(script_args)
#     subprocess.run(cmd, check=True)

def run_python_script(script_path, script_args=None, **kwargs):
    cmd = [sys.executable, '-u', script_path]  # -u = unbuffered
    if script_args:
        cmd.extend(script_args)

    logger.info(f"Running command: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # merge stderr into stdout
        text=True,
        env={**os.environ, 'PYTHONUNBUFFERED': '1'}
    )

    # Relay all script output into Airflow task logs
    for line in result.stdout.splitlines():
        logger.info(line)

    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)


script_exec_cmd = build_python_cmd(config['script_path'])

script_exec_cmd_silver = build_python_cmd(config['script_path'], '--layer', 'silver')

script_exec_cmd_gold = build_python_cmd(config['script_path'], '--layer', 'gold')

dag = DAG(
    config['dag_nm'],
    default_args=default_args,
    schedule_interval=config['cron'],
    max_active_runs=1,
    catchup=False,
    tags=config['tags'],
    params={'mcd_connection_id': f'mcd_gateway_conn_{env}'},
    sla_miss_callback=mcd_callbacks.mcd_sla_miss_callback,
    on_failure_callback=[mcd_callbacks.mcd_dag_failure_callback],  # MC DAG Failure Callback
    on_success_callback=[message.on_success_callback, mcd_callbacks.mcd_dag_success_callback],
    # Slack/Email + MC DAG Success Callback
    doc_md=__doc__,
)

start_task = DummyOperator(task_id='start', dag=dag)

run_digital_youtube_silver_task = PythonOperator(
    task_id='digital_youtube_silver_task',
    python_callable=run_python_script,
    op_kwargs=script_exec_cmd_silver,
    dag=dag,
)

run_digital_youtube_gold_task = PythonOperator(
    task_id='digital_youtube_gold_task',
    python_callable=run_python_script,
    op_kwargs=script_exec_cmd_gold,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)

start_task >> run_digital_youtube_silver_task >> run_digital_youtube_gold_task >> end_task

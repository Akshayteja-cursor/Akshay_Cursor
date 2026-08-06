import pendulum
from datetime import datetime, timedelta

adsales_digital_youtube_config = {
    "dev": {
        "dag_default_config": {
            "depends_on_past": False,
            "owner": "adsales",
            "retry_delay": timedelta(minutes=2),
            "retries": 2,
            "email_on_retry": False,
            "start_date": datetime(2020, 4, 24, tzinfo=pendulum.timezone("US/Pacific")),
            },
        "email_config": {
            "from_email": "adsales.dev@data.fox",
            "to_email_success": "shubham.nasare@fox.com",
            "to_email_failure": "shubham.nasare@fox.com",
            },
        "dag_nm": "adsales_digital_youtube",
        "cron": '30 11 * * *',
        "dbt_job_id_silver": 829087,
        "dbt_step_name_silver": "qa_adsales_silver_youtube",
        "dbt_job_id_gold": 834394,
        "dbt_step_name_gold": "qa_adsales_gold_youtube",
        "script_path": "/usr/local/airflow/include/adsales/scripts/adsales_digital_youtube/adsales_digital_youtube_new.py",
        "dbt_conn_id": "adsales-dbt-conn-dev",
        "server_hostname": "dbc-c492c482-09a1.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/bff83285e02f19a6",
        "aws_conn_id":"adsales-aws-conn-dev",
        "aws_region": "us-west-2",
        "tags": ["adsales", "digital"],
        "email_s3_bucket": "fdp-dmo-emails-test",
        "email_s3_key": "adsales/dev/youtube",
        "file_list": ["news_views_daily", "entertainment_views_daily", "sports_views_daily", "outkick_views_daily", "tmz_views_daily",
                      "news_views_monthly", "entertainment_views_monthly", "sports_views_monthly", "outkick_views_monthly", "tmz_views_monthly",
                      "news_revenue_daily", "entertainment_revenue_daily", "sports_revenue_daily", "outkick_revenue_daily", "tmz_revenue_daily",
                      "news_revenue_monthly", "entertainment_revenue_monthly", "sports_revenue_monthly", "outkick_revenue_monthly", "tmz_revenue_monthly"],
        "email_env": "dev",
        "data_s3_bucket": "fox-fdp-bi-dev",
        # Must match QA dbt/external stage location. JIRA BIADS-21813 Step 1 left stage
        # tables on the QA path; Support drops the same daily files onto that path.
        # Writing to adsales/dev/youtube while dbt reads adsales/qa/youtube causes
        # Prod vs QA row-count drift even when identical source files are sent.
        "data_s3_key": "adsales/qa/youtube"
        },
    "prod": {
        "dag_default_config": {
            "depends_on_past": False,
            "owner": "adsales",
            "retry_delay": timedelta(minutes=2),
            "retries": 2,
            "email_on_retry": False,
            "start_date": datetime(2020, 4, 24, tzinfo=pendulum.timezone("US/Pacific")),
        },
        "email_config": {
            "from_email": "adsales.prod@data.fox",
            "to_email_success": "fox.ai@fox.com",
            "to_email_failure": "fox.ai@fox.com, dataproductsupport@fox.com",
        },
        "dag_nm": "adsales_digital_youtube",
        "cron": '30 6 * * *',
        "dbt_job_id_silver": 70437463715378,
        "dbt_step_name_silver": "prod_adsales_silver_youtube",
        "dbt_job_id_gold": 70437463715377,
        "dbt_step_name_gold": "prod_adsales_gold_youtube",
        "script_path": "/usr/local/airflow/include/adsales/scripts/adsales_digital_youtube/adsales_digital_youtube_new.py",
        "dbt_conn_id": "adsales-dbt-conn-prod",
        "server_hostname": "dbc-c492c482-09a1.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/bff83285e02f19a6",
        "aws_conn_id":"adsales-aws-conn-prod",
        "aws_region": "us-west-2",
        "tags": ["adsales", "digital"],
        "email_s3_bucket": "fdp-dmo-emails-prod",
        "email_s3_key": "adsales/prod/youtube",
        "file_list": ["news_views_daily", "entertainment_views_daily", "sports_views_daily", "outkick_views_daily", "tmz_views_daily",
                      "news_views_monthly", "entertainment_views_monthly", "sports_views_monthly", "outkick_views_monthly", "tmz_views_monthly",
                      "news_revenue_daily", "entertainment_revenue_daily", "sports_revenue_daily", "outkick_revenue_daily", "tmz_revenue_daily",
                      "news_revenue_monthly", "entertainment_revenue_monthly", "sports_revenue_monthly", "outkick_revenue_monthly", "tmz_revenue_monthly"],
        "email_env": "prod",
        "data_s3_bucket": "fox-fdp-bi-prod",
        "data_s3_key": "adsales/prod/youtube"
        }
}

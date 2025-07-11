from prefect import flow, task
from prefect.blocks.system import Secret
from dbtmetabase import DbtMetabase, Filter
import requests
import json


DBT_ACCOUNT_ID=
DBT_DAILY_JOB_ID=

METABASE_URL="" # your metabase url
METABASE_DATABASE="" # the title of your database connection in metabase
DBT_DATABASES_TO_SYNC=["your_db_name"] # list of databases from dbt manifest file

dbt_secret_block=Secret.load("your_prefect_secret_block")
metabase_secret_block=Secret.load("your_metabase-secret_block")

@task()
# returns the most recent run from the daily job
def get_most_recent_daily_run_id():
    url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/"
    querystring = {"job_definition_id":f"{DBT_DAILY_JOB_ID}","limit":"1","order_by":"-finished_at"}
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + dbt_secret_block.get()
    }
    response = requests.get(url, headers=headers, params=querystring)
    json_response = json.loads(response.text)
    latest_run_id = json_response['data'][0]['id']
    return(latest_run_id)

@task()
def write_most_recent_daily_run_artifacts():
    run_id = get_most_recent_daily_run_id()
    artifacts_url = f'https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/artifacts/manifest.json'
    headers = {"Authorization": "Bearer " + dbt_secret_block.get()}
    response = requests.get(artifacts_url, headers=headers)

    with open("manifest.json", "w") as f:
        f.write(response.text)

@flow(name="Enrich Metabase w dbt descriptions")
def enrich_metabase_w_dbt_descriptions():
    write_most_recent_daily_run_artifacts()
    conn = DbtMetabase(
        manifest_path="manifest.json",
        metabase_url=METABASE_URL,
        metabase_api_key=metabase_secret_block.get(),
    )
    conn.export_models(
        metabase_database="Cypress Analytics (Snowflake)",
        database_filter=Filter(DBT_DATABASES_TO_SYNC), 
        sync_timeout=300
    )

if __name__ == "__main__":
    enrich_metabase_w_dbt_descriptions() # runs enrichment (manually)


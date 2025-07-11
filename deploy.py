from prefect.deployments.runner import DeploymentImage
from prefect.logging import get_logger
from src.enrich_metabase_w_dbt_data import enrich_metabase_w_dbt_descriptions

IMAGE_NAME = ""
IMAGE_TAG = "latest"
CLOUDWATCH_LOGS_PATH = ""

image: DeploymentImage = DeploymentImage(
    name=IMAGE_NAME,
    tag=IMAGE_TAG,
    dockerfile="./Dockerfile",
)

if __name__ == "__main__":
    logger = get_logger("deploy metabase enrichment")

    logger.info("Deploying Daily Metabase Enrichment Job Prod Flow...")
    enrich_metabase_w_dbt_descriptions.deploy(
        name="DBT Metabase Enrichment Daily Job - Prod",
        work_pool_name="Prod Push Workpool",
        image=image,
        schedule={"cron": "0 9 * * *", "timezone": "America/New_York"},
        tags=["production"],
        build=False,
        job_variables={
            "cloudwatch_logs_options": {"awslogs-stream-prefix": CLOUDWATCH_LOGS_PATH}
        },
    )

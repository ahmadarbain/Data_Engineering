from prefect.deployments import Deployment 
from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_web_to_gcs

github_block = GitHub.load("etl-github")

github_deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name='Load to GCS (github)',
    storage=github_block
)

github_deployment.apply()
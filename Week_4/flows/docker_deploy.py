from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow import web_to_gcs

docker_container_block = DockerContainer.load("dbt-project")

docker_dep = Deployment.build_from_flow(
    flow=web_to_gcs,
    name="dbt_project",
    infrastructure=docker_container_block,
)


if __name__ == "__main__":
    docker_dep.apply()
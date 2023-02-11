from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image = "222122/prefect:zoom",
    image_pull_policy = "ALWAYS",
    auto_remove = True
)

docker_block.save("zoom", overwrite=True)
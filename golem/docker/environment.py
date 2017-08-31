from golem.environments.environment import Environment, SupportStatus


class DockerEnvironment(Environment):

    def __init__(self, docker_images):
        """
        :param list(DockerImage) docker_images: nonempty list of Docker images,
          at least one should be available for this environment to be supported.
        :return:
        """
        if docker_images is None:
            raise AttributeError("docker_images is None")
        self.docker_images = docker_images
        Environment.__init__(self)
        self.software.append('Docker')

    def check_docker_images(self) -> SupportStatus:
        if any(img.is_available() for img in self.docker_images):
            return SupportStatus.ok()

        return SupportStatus.err({'environment_unsupported': {
            'env_id': self.get_id(),
            'docker_images_missing_any': self.docker_images,
        }})

    def supported(self) -> SupportStatus:
        return self.check_docker_images().join(Environment.supported(self))

    def description(self):
        descr = Environment.description(self)

        descr += "DOCKER IMAGES (any of):\n"
        for img in self.docker_images:
            descr += "\t * " + img.name + "\n"
        descr += "\n"

        return descr

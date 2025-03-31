FROM ghcr.io/facebookincubator/velox-dev:adapters
RUN yum install -y sudo patch maven perl
RUN ln -s /usr/local/bin/cmake /usr/bin

ENV CUDA_ARCHITECTURES=70


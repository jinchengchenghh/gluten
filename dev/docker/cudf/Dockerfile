FROM ghcr.io/facebookincubator/velox-dev:adapters
RUN yum install -y sudo
RUN dnf install perl
RUN yum install -y patch
RUN ln -s /usr/local/bin/cmake /usr/bin
RUN yum install -y maven

ENV CUDA_ARCHITECTURES=70


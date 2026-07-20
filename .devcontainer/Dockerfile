FROM docker.io/rust:1-trixie

ENV USERNAME=dev
ENV USER_UID=1000
ENV USER_GID=1000

RUN apt update && \
    apt install -y vim make git protobuf-compiler just

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd -m -s /bin/bash --uid $USER_UID --gid $USER_GID $USERNAME

USER $USERNAME
WORKDIR /workspace

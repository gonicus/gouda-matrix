FROM docker.io/rust:1-trixie

RUN apt update && \
    apt install -y vim make git protobuf-compiler

RUN useradd -ms /bin/bash devuser

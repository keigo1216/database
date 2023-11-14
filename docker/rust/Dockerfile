FROM rust

ENV CARGO_TARGET_DIR=/tmp/target

WORKDIR /usr/src/

RUN apt-get update && \
    apt-get upgrade -y 

RUN rustup self update && \
    rustup update stable && \
    rustup component add rustfmt


WORKDIR /usr/src/
CMD ["/bin/bash"]
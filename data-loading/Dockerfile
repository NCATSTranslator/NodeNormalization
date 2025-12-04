# Use Redis so we can redis-cli.
FROM redis:8

# Configuration
ARG ROOT=/home/nru
ARG DATA=/data

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install wget python3-venv python3-pip

# Create nru user
RUN mkdir -p ${ROOT}
RUN useradd nru -d ${ROOT}
RUN chown nru ${ROOT}
WORKDIR ${ROOT}

# Set up a volume for data.
VOLUME ${DATA}
RUN mkdir -p ${DATA}
RUN chown nru ${DATA}

# Switch to nru user.
USER nru

# Set up VENV
ENV VIRTUAL_ENV=${ROOT}/venv
RUN python3 -m venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:$PATH"

# Install requirements.
COPY --chown=nru requirements.txt ${ROOT}
RUN pip3 install -r requirements.txt

# Entrypoint should be bash.
ENTRYPOINT /bin/bash

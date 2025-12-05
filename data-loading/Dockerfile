# Use Redis so we can redis-cli.
FROM redis:6

# Configuration
ARG ROOT=/home/nru
ARG DATA=/data

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install wget python3-venv python3-pip
RUN apt-get -y install git gcc

# Create nru user
RUN mkdir -p ${ROOT}
RUN useradd nru -d ${ROOT}
RUN chown nru ${ROOT}
WORKDIR ${ROOT}

# We need to install rdb-cli, which we need to -- lolsob -- compile from source. So let's do that.
RUN git clone https://github.com/redis/librdb.git
WORKDIR ${ROOT}/librdb
RUN make install
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

# Copy scripts.
COPY --chown=nru rdb-to-resp.sh ${ROOT}

# Make sure rdb-cli has been set up.
# RUN rdb-cli

# Entrypoint should be bash.
ENTRYPOINT ["/bin/bash"]

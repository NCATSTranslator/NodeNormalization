FROM ghcr.io/translatorsri/renci-python-image:3.11.5

RUN mkdir /code
WORKDIR /code

# install library
COPY ./requirements.txt requirements.txt
COPY ./requirements-loader.txt requirements-loader.txt
COPY ./node_normalizer node_normalizer
COPY ./config.json config.json
COPY ./redis_config.yaml redis_config.yaml
COPY ./load.py load.py

# install requirements (frontend + loader; the loader Helm chart runs load.py
# from this image)
RUN pip install -r requirements.txt -r requirements-loader.txt

RUN chmod 777 ./

USER nru
ENTRYPOINT ["uvicorn", "--host", "0.0.0.0", "--port", "8080", "--root-path", "/1.3", "--workers", "1", "node_normalizer.server:app"]

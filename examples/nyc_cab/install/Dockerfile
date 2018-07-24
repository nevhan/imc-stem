FROM daskdev/dask:latest
COPY ./requirements.txt ./requirements.txt
RUN /opt/conda/bin/pip install -r requirements.txt && conda clean -tipsy

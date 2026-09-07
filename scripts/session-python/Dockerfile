FROM condaforge/miniforge3:25.3.1-0

RUN pip install --no-cache-dir \
      ipykernel==6.30.1 \
      jupyter-client==8.6.3 \
      fsspec==2025.7.0 \
      s3fs==2025.7.0

COPY scripts/session-helper/session-helper /opt/aruna/session-helper
COPY scripts/session-helper/session-start /opt/aruna/session-start
RUN chmod 0755 /opt/aruna/session-helper /opt/aruna/session-start \
    && python -m ipykernel install --name python3 --prefix /usr/local

ENV ARUNA_SESSION_KERNEL=python3
ENV JUPYTER_PATH=/usr/local/share/jupyter
WORKDIR /work
USER 65534:65534
ENTRYPOINT []
CMD ["/opt/aruna/session-start"]

FROM denoland/deno:bin-2.9.3 AS deno

FROM python:3.13-slim-bookworm

COPY --from=deno /deno /usr/local/bin/deno

RUN pip install --no-cache-dir \
      jupyter-client==8.6.3 \
      fsspec==2025.7.0 \
      s3fs==2025.7.0

COPY scripts/session-helper/session-helper /opt/aruna/session-helper
COPY scripts/session-helper/session-start /opt/aruna/session-start
# Deno ships its own kernel; registering its kernelspec lets the same helper
# drive it through jupyter_client. It goes to a shared path because the
# container runs as an unprivileged uid that cannot read root's home.
RUN chmod 0755 /opt/aruna/session-helper /opt/aruna/session-start \
    && DENO_DIR=/opt/aruna/.deno-cache JUPYTER_DATA_DIR=/usr/local/share/jupyter \
       deno jupyter --install \
    && chmod -R a+rX /usr/local/share/jupyter /opt/aruna/.deno-cache

ENV ARUNA_SESSION_KERNEL=deno
ENV DENO_DIR=.deno-cache
ENV JUPYTER_PATH=/usr/local/share/jupyter
WORKDIR /work
USER 65534:65534
ENTRYPOINT []
CMD ["/opt/aruna/session-start"]

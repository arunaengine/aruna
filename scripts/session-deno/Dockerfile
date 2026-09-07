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
    && deno jupyter --install \
    && jupyter kernelspec install /root/.local/share/jupyter/kernels/deno --prefix=/usr/local \
    && chmod -R a+rX /usr/local/share/jupyter

RUN python3 - <<'PY'
import json
from pathlib import Path
spec = Path('/usr/local/share/jupyter/kernels/deno/kernel.json')
spec.write_text(json.dumps(json.loads(spec.read_text()) | {"interrupt_mode": "message"}))
PY

ENV ARUNA_SESSION_KERNEL=deno
ENV DENO_DIR=.deno-cache
ENV JUPYTER_PATH=/usr/local/share/jupyter
WORKDIR /work
USER 65534:65534
ENTRYPOINT []
CMD ["/opt/aruna/session-start"]

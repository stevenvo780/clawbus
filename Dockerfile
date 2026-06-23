# clawbus broker — minimal image.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY clawbus/ ./clawbus/
COPY deploy/ ./deploy/

RUN pip install --no-cache-dir websockets

ENV CLAWBUS_STATE_DIR=/var/lib/clawbus
VOLUME ["/var/lib/clawbus"]
EXPOSE 18791

CMD ["python", "-m", "clawbus", "broker"]

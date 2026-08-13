# anvil-events — thin container over the same `serve` verb (ADR-0002)
# Build: docker build -f deploy/Dockerfile -t anvil-events .
# The journal root is a MOUNTED VOLUME on the host; the container is stateless.
FROM python:3.12-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir .

EXPOSE 9877
VOLUME ["/var/lib/anvil/events"]

ENV ANVIL_EVENTS_NATS_URL=nats://127.0.0.1:4222
ENV ANVIL_EVENTS_ROOT=/var/lib/anvil/events

ENTRYPOINT ["anvil-events", "serve", "--root", "/var/lib/anvil/events"]

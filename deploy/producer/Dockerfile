# StreamGuard producer image (also used by producer-seed). Build context: repo root.
# Code is not copied: ../src is mounted read-only, so a code change needs only a restart.
FROM python:3.13-slim

WORKDIR /opt/streamguard

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/opt/streamguard/src \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "streamguard.producer"]
CMD ["--rate", "200", "--burst-rate", "1000", "--burst-every", "60", "--burst-seconds", "5", \
     "--seed", "42"]

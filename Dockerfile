FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY collectors/ collectors/

# One image for all collectors; pick which one with the command,
# e.g. `docker run <image> collectors.vehicle_positions`
ENTRYPOINT ["python", "-m"]

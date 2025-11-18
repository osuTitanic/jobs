FROM python:3.14-alpine AS builder

ENV PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apk add --no-cache \
    build-base \
    cargo \
    libffi-dev \
    openssl-dev \
    pkgconf \
    postgresql-dev \
    rust \
    zlib-dev \
    git

WORKDIR /tmp/build
COPY requirements.txt ./
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --no-compile --root /install -r requirements.txt

FROM python:3.14-alpine

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on

RUN apk add --no-cache \
    ca-certificates \
    libffi \
    libstdc++ \
    openssl \
    postgresql-libs \
    zlib

COPY --from=builder /install/usr/local /usr/local

WORKDIR /jobs
COPY . .

RUN python -m compileall -q app

STOPSIGNAL SIGTERM
CMD ["python", "main.py"]
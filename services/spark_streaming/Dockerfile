FROM apache/spark:3.5.1

USER root

# ---- Python 3.11 for PySpark + ML deps ----
# The base image is Ubuntu 20.04 and ships with Python 3.8.
# Our pickled sklearn model requires numpy>=2 (it references numpy._core.*), so
# we must run PySpark with Python 3.11 (numpy>=2 doesn't support Python 3.8).
#
# Ubuntu 20.04 doesn't have python3.11 packages in official repos anymore, so we
# compile a pinned CPython release from source for deterministic builds.
ARG PYTHON_VERSION=3.11.8
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    build-essential \
    zlib1g-dev libssl-dev libbz2-dev libreadline-dev libsqlite3-dev \
    libffi-dev liblzma-dev tk-dev uuid-dev \
  && curl -fsSL "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" -o /tmp/Python.tgz \
  && tar -xzf /tmp/Python.tgz -C /tmp \
  && cd "/tmp/Python-${PYTHON_VERSION}" \
  && ./configure --prefix=/usr/local --enable-shared --with-ensurepip=install \
  && make -j"$(nproc)" \
  && make altinstall \
  && ldconfig \
  && cd / \
  && rm -rf "/tmp/Python-${PYTHON_VERSION}" /tmp/Python.tgz \
  && /usr/local/bin/python3.11 -m pip install --no-cache-dir --upgrade pip setuptools wheel \
  && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=/usr/local/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

WORKDIR /opt/spark-app
COPY requirements.txt /tmp/requirements.txt
RUN /usr/local/bin/python3.11 -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY app /opt/spark-app/app

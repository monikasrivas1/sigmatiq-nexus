FROM python:3.11-slim

# Install system dependencies for high-performance math
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package metadata and source before installing the local package.
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir hatchling && \
    pip install --no-cache-dir .

# Copy models after dependency install so code-only changes reuse dependency layers.
COPY models/ models/

# Run as non-privileged user for security
RUN useradd -m sigmatiq
USER sigmatiq

# Ensure models are in the right path for worker.py
ENV PYTHONPATH=/app/src
ENV MODEL_TCN_PATH=/app/models/tcn_encoder_v1.onnx
ENV MODEL_RL_PATH=/app/models/rl/cql_policy_v1.onnx

CMD ["python", "-m", "sigmatiq_nexus.nexus_worker"]

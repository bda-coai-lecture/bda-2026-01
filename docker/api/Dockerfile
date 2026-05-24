FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONPATH=/app/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN uv pip install --system --no-cache \
    "fastapi>=0.136.1" \
    "uvicorn>=0.46.0" \
    "lightgbm>=4.6.0" \
    "numpy>=2.3.0" \
    "pandas>=2.2,<3" \
    "pyarrow>=23.0.1" \
    "scikit-learn>=1.8.0"

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

EXPOSE 8000

CMD ["uvicorn", "ghrec.api:app", "--host", "0.0.0.0", "--port", "8000"]

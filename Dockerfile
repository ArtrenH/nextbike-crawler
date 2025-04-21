FROM python:3.12-slim AS rust-python

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl git build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Rust using rustup (non-interactive)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

FROM rust-python AS builder

WORKDIR /app

COPY fastjsonpatch/ fastjsonpatch/
COPY jsoncache/ jsoncache/

RUN pip install --no-cache-dir fastjsonpatch/
RUN pip install --no-cache-dir jsoncache/

FROM python:3.12-slim

WORKDIR /app

# Copy the installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/

COPY crawler.py ./

CMD ["python", "crawler.py"]
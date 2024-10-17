FROM quay.io/astronomer/astro-runtime:12.1.1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER astro
FROM ghcr.io/coder/code-server:4.92.2

# Install Python, pip, sqlite3, postgresql-client and build essentials
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        sqlite3 \
        postgresql-client \
        git \
        curl \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment for Python tooling
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Upgrade pip and install dbt with postgres and sqlite adapters in venv
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir \
        "dbt-core>=1.6,<2.0" \
        "dbt-postgres>=1.6,<2.0" \
        "dbt-sqlite>=1.6,<2.0" \
        "psycopg2-binary>=2.9,<3.0" \
        "pandas>=2.0,<3.0" \
        "sqlalchemy>=2.0,<3.0"

# Ensure login shells and terminals see the venv on PATH
RUN echo 'export PATH="/opt/venv/bin:$PATH"' > /etc/profile.d/venv-path.sh \
    && chmod 0755 /etc/profile.d/venv-path.sh \
    && ln -sf /opt/venv/bin/dbt /usr/local/bin/dbt

# Note: Extensions will be installed at startup by start.sh as the coder user
# to ensure they're in the correct user profile

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/home/coder/project

WORKDIR /home/coder/project

# Startup script to init dbt and launch code-server
COPY start.sh /usr/local/bin/start.sh
RUN chmod +x /usr/local/bin/start.sh

# code-server listens on 8080 by default in this image
EXPOSE 8080

# The docker-compose will set the command to run start.sh
USER coder


FROM ghcr.io/coder/code-server:4.92.2

# Install Python, pip, sqlite3 and build essentials
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        sqlite3 \
        git \
        curl \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment for Python tooling
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Upgrade pip and install dbt with sqlite adapter in venv
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir \
        "dbt-core>=1.6,<2.0" \
        "dbt-sqlite>=1.6,<2.0"

# Ensure login shells and terminals see the venv on PATH
RUN echo 'export PATH="/opt/venv/bin:$PATH"' > /etc/profile.d/venv-path.sh \
    && chmod 0755 /etc/profile.d/venv-path.sh \
    && ln -sf /opt/venv/bin/dbt /usr/local/bin/dbt

# Pre-install helpful VS Code extensions (Open VSX registry)
RUN code-server --install-extension innoverio.vscode-dbt-power-user --force || true \
    && code-server --install-extension ms-python.python --force || true \
    && code-server --install-extension ms-toolsai.jupyter --force || true \
    && code-server --install-extension eamodio.gitlens --force || true

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


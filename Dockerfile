FROM quay.io/astronomer/astro-runtime:10.9.0

# Set PYTHONPATH so Airflow finds your code
ENV PYTHONPATH="/usr/local/airflow:${PYTHONPATH}"

# AWS Region configuration
# Note: AWS credentials should be provided at runtime via:
# - Environment variables passed to the container
# - AWS credentials file mounted into the container
# - IAM roles (if running in AWS)
ENV AWS_DEFAULT_REGION=us-east-1

# Database environment variables
ENV BDI_DB_HOST=3.82.171.80
ENV BDI_DB_PORT=5432
ENV BDI_DB_USERNAME=myuser
ENV BDI_DB_PASSWORD=mypassword
ENV BDI_DB_DATABASE=aircraft_db

# Install AWS CLI and PostgreSQL client libraries
USER root
RUN apt-get update && apt-get install -y \
    awscli \
    libpq-dev

# Return to astro user
USER astro

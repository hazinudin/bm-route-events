# Start from the official migrate image
FROM migrate/migrate:latest

# Set working directory
WORKDIR /migrations

# Copy all migration files into the container
COPY . /migrations

# Default entrypoint (no hardcoded env vars)
ENTRYPOINT ["migrate"]
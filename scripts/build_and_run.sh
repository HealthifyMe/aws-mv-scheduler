#!/bin/bash

# Build docker image
docker build -t mv-scheduler .

# Run container with environment variables
docker run -d \
  -p 3000:3000 \
  -v $(pwd)/logs:/app/logs \
  --env-file .env \
  -e LOG_DIR=/app/logs \
  --restart unless-stopped \
  --name mv-scheduler \
  mv-scheduler 
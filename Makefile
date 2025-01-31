.PHONY: build run stop clean logs test lint

# Docker image name
IMAGE_NAME = mv-scheduler

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Run the container
run:
	docker run -d \
		-p 3000:3000 \
		-v $(PWD)/logs:/app/logs \
		--env-file .env \
		-e LOG_DIR=/app/logs \
		--restart unless-stopped \
		--name $(IMAGE_NAME) \
		$(IMAGE_NAME)

# Stop and remove the container
stop:
	docker stop $(IMAGE_NAME) || true
	docker rm $(IMAGE_NAME) || true

# Clean up Docker resources
clean: stop
	docker rmi $(IMAGE_NAME) || true

# View logs
logs:
	docker logs -f $(IMAGE_NAME)

# Run tests
test:
	python -m pytest tests/

# Install development dependencies
dev-setup:
	pip install -r requirements.txt
	pip install pytest pylint black

# Format code using black
format:
	black src/ tests/

# Run pylint
lint:
	pylint src/ tests/

# Start fresh (clean, build, run)
start-fresh: clean build run

# Show help
help:
	@echo "Available commands:"
	@echo "  make build        - Build Docker image"
	@echo "  make run          - Run container"
	@echo "  make stop         - Stop container"
	@echo "  make clean        - Remove container and image"
	@echo "  make logs         - View container logs"
	@echo "  make test         - Run tests"
	@echo "  make dev-setup    - Install development dependencies"
	@echo "  make format       - Format code using black"
	@echo "  make lint         - Run pylint"
	@echo "  make start-fresh  - Clean, build, and run" 
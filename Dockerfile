FROM python:3.9-slim-buster

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the application
COPY . .

# Create logs directory if it doesn't exist
RUN mkdir -p logs

# Run both scheduler and processor
CMD ["sh", "-c", "python -m src.core.scheduler & python -m src.core.processor & wait"]

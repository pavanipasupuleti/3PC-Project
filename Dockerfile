FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Create data directory for SQLite
RUN mkdir -p data

# Expose ports
EXPOSE 5000 5001 5002 5003 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:5000/health')" || exit 1

CMD ["python3", "-m", "coordinator.server"]
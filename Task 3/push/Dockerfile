# Dockerfile
# Use the official lightweight Python image.
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy local code to the container image.
COPY . .

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Run the web service on container startup.
# Gunicorn is used for production deployment.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
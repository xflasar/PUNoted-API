# Use an official lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing pyc files to disc
ENV PYTHONDONTWRITEBYTECODE=1
# Prevent Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies (needed for some python packages)
RUN apt-get update && apt-get install -y gcc

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn uvicorn

# Copy project
COPY . .

# Expose the port Gunicorn will run on
EXPOSE 8000

# Run Gunicorn
# -w 4: Number of workers (adjust based on CPU)
# -k uvicorn.workers.UvicornWorker: Use Uvicorn class
# -b 0.0.0.0:8000: Bind to all interfaces inside container
CMD ["gunicorn", "main:app", \
     "-w", "6", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "-b", "0.0.0.0:9901", \
     "--timeout", "300", \
     "--keep-alive", "5"]
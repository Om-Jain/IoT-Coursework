# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements (if you have one)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your processor code
COPY . .

# Set environment variables (optional)
ENV PYTHONUNBUFFERED=1

# Run the processor script
CMD ["python", "processor.py"]
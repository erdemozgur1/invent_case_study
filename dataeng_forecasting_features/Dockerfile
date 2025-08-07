# Use slim Python image
FROM python:3.9.13-slim

# Set working directory inside container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files into container
COPY . .

# Set default command
ENTRYPOINT ["python", "solution.py"]

FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project to the working directory
COPY . .

# Run Gunicorn with 3 worker processes
CMD ["gunicorn", "-w", "3", "-b", "0.0.0.0:8000", "app:app"]

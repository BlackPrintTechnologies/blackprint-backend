#!/bin/bash

# Define variables
REPO_DIR="backend/blackprint-backend"
IMAGE_NAME="flask-app"
CONTAINER_NAME="flask-app"
PORT_MAPPING="8000:8000"

# Pull the latest changes from the Git repository
echo "Pulling latest changes from Git repository..."
git pull || { echo "Failed to pull latest changes"; exit 1; }

# Build a new Docker image
echo "Building new Docker image..."
docker build -t $IMAGE_NAME . || { echo "Failed to build Docker image"; exit 1; }

# Stop and remove the existing Docker container
echo "Stopping and removing existing Docker container..."
docker stop $CONTAINER_NAME || { echo "Failed to stop Docker container"; exit 1; }
docker rm $CONTAINER_NAME || { echo "Failed to remove Docker container"; exit 1; }

# Run a new Docker container with the updated image
echo "Running new Docker container..."
docker run -d -p $PORT_MAPPING --name $CONTAINER_NAME $IMAGE_NAME || { echo "Failed to run Docker container"; exit 1; }

echo "Update and deployment complete."
sudo docker run -d -p 8000:8000 --name flask-app flask-app

#!/bin/bash

# Blackprint Backend Deployment Script with Redis
# For EC2 with 4GB RAM and 40GB storage

echo "🚀 Starting Blackprint Backend deployment with Redis..."

# Update system
echo "📦 Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install Docker and Docker Compose
echo "🐳 Installing Docker and Docker Compose..."
sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER

# Create app directory
echo "📁 Setting up application directory..."
mkdir -p /home/ubuntu/blackprint-backend
cd /home/ubuntu/blackprint-backend

# Copy application files (assuming you have them)
# You'll need to copy your application files here

# Create .env file for environment variables
echo "🔧 Creating environment configuration..."
cat > .env << EOF
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
FLASK_ENV=production
EOF

# Set proper permissions
echo "🔐 Setting permissions..."
sudo chown -R $USER:$USER /home/ubuntu/blackprint-backend
chmod +x deploy_with_redis.sh

# Start services with Docker Compose
echo "🚀 Starting services with Docker Compose..."
docker-compose up -d

# Check if services are running
echo "🔍 Checking service status..."
docker-compose ps

# Show logs
echo "📋 Recent logs:"
docker-compose logs --tail=20

echo "✅ Deployment completed!"
echo "🔧 Redis is running on port 6379"
echo ""
echo "📊 Useful commands:"
echo "  Check Redis: docker-compose exec redis redis-cli ping" 
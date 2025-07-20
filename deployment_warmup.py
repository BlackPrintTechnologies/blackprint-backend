#!/usr/bin/env python3
"""
Deployment warmup script
Run this after deployment to ensure the application is fully warmed up
"""
import requests
import time
import sys

def check_health(base_url="http://localhost:5002", max_attempts=30):
    """Check if the application is warmed up and ready"""
    print("🔄 Checking application health and warmup status...")
    
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(f"{base_url}/health", timeout=10)
            data = response.json()
            
            if response.status_code == 200 and data.get('status') == 'healthy':
                print(f"✅ Application is healthy and warmed up! (attempt {attempt})")
                print(f"📊 Warmup details: {data.get('warmup_details', {})}")
                return True
            elif response.status_code == 202:
                print(f"🔄 Application is warming up... (attempt {attempt}/{max_attempts})")
                print(f"📊 Status: {data.get('warmup_details', {})}")
                time.sleep(2)
            else:
                print(f"⚠️ Unexpected response: {response.status_code} - {data}")
                time.sleep(2)
                
        except requests.exceptions.RequestException as e:
            print(f"🔌 Connection failed (attempt {attempt}/{max_attempts}): {e}")
            time.sleep(2)
    
    print(f"❌ Application failed to warm up after {max_attempts} attempts")
    return False

def warm_up_endpoints(base_url="http://localhost:5002"):
    """Make sample requests to warm up common endpoints"""
    print("🔥 Making warmup requests to common endpoints...")
    
    warmup_endpoints = [
        "/health",
        # Add other endpoints you want to warm up
    ]
    
    for endpoint in warmup_endpoints:
        try:
            print(f"🎯 Warming up {endpoint}...")
            response = requests.get(f"{base_url}{endpoint}", timeout=10)
            print(f"✅ {endpoint} responded with status {response.status_code}")
        except Exception as e:
            print(f"⚠️ Failed to warm up {endpoint}: {e}")

def main():
    """Main warmup process"""
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5002"
    
    print(f"🚀 Starting deployment warmup for {base_url}")
    
    # Check if application is healthy and warmed up
    if check_health(base_url):
        # Make additional warmup requests
        warm_up_endpoints(base_url)
        print("🎉 Deployment warmup completed successfully!")
        return 0
    else:
        print("❌ Deployment warmup failed!")
        return 1

if __name__ == "__main__":
    exit(main()) 
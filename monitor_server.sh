#!/bin/bash
# Kafka MCP Server Monitoring Script

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔍 Kafka MCP Server Monitor"
echo "=========================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate || source venv/Scripts/activate

# Load environment
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

# Run health check
echo "Running health check..."
python -c "
import asyncio
import sys
import os
sys.path.append('.')

async def health_check():
    try:
        from kafka_mcp_server import mcp
        # Simple connectivity test
        print('✅ Server imports successfully')
        
        # Test Kafka connectivity
        from kafka import KafkaAdminClient
        kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
            'security_protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
        }
        kafka_config = {k: v for k, v in kafka_config.items() if v is not None}
        
        admin = KafkaAdminClient(**kafka_config)
        metadata = admin.describe_cluster()
        admin.close()
        
        print(f'✅ Connected to Kafka cluster: {metadata.cluster_id or \"unknown\"}')
        print(f'📊 Brokers: {len(metadata.brokers)}')
        
    except Exception as e:
        print(f'❌ Health check failed: {e}')
        return False
    
    return True

if __name__ == '__main__':
    result = asyncio.run(health_check())
    sys.exit(0 if result else 1)
"

echo "Monitor complete."

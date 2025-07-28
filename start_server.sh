#!/bin/bash
# Kafka MCP Server Startup Script

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment from .env"
fi

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate || source venv/Scripts/activate
    echo "Activated virtual environment"
fi

# Start the server
echo "Starting Kafka MCP Server..."
python kafka_mcp_server.py "$@"

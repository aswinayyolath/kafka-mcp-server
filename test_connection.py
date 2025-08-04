#!/usr/bin/env python3
"""
Simple script to test Kafka connectivity for debugging MCP server issues.
"""

import os
import sys
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable, KafkaError

def test_kafka_connection():
    """Test connection to Kafka cluster."""
    
    # Configuration
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094').split(',')
    security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
    
    print(f"Testing Kafka connection...")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Security protocol: {security_protocol}")
    print("-" * 50)
    
    try:
        # Create admin client
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            request_timeout_ms=10000,
            api_version=(2, 6, 0)
        )
        
        # Test cluster metadata
        metadata = admin.describe_cluster()
        print(f"✅ Connection successful!")
        
        # Handle both dict and object formats
        if isinstance(metadata, dict):
            cluster_id = metadata.get('cluster_id', 'N/A')
            controller = metadata.get('controller_id', 'N/A')
            brokers = metadata.get('brokers', [])
            print(f"Cluster ID: {cluster_id}")
            print(f"Controller: {controller}")
            print(f"Brokers ({len(brokers)}):")
            
            for broker in brokers:
                node_id = broker.get('node_id', 'N/A')
                host = broker.get('host', 'N/A')
                port = broker.get('port', 'N/A')
                print(f"  - Broker {node_id}: {host}:{port}")
        else:
            print(f"Cluster ID: {metadata.cluster_id}")
            print(f"Controller: {metadata.controller}")
            print(f"Brokers ({len(metadata.brokers)}):")
            
            for broker in metadata.brokers:
                print(f"  - Broker {broker.nodeId}: {broker.host}:{broker.port}")
        
        # List topics
        topics = admin.list_topics()
        print(f"\nTopics ({len(topics)}):")
        for topic in sorted(topics):
            print(f"  - {topic}")
        
        admin.close()
        return True
        
    except NoBrokersAvailable as e:
        print(f"❌ No brokers available: {e}")
        print("This usually means:")
        print("  1. Kafka cluster is not running")
        print("  2. Wrong bootstrap servers configuration")
        print("  3. Network connectivity issues")
        return False
        
    except KafkaError as e:
        print(f"❌ Kafka error: {e}")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_kafka_connection()
    sys.exit(0 if success else 1)

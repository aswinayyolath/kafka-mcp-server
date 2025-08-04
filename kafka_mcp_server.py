#!/usr/bin/env python3
"""
Kafka MCP Server - Complete Implementation
A comprehensive Model Context Protocol server for Kafka operations.

Features:
- Cluster management and monitoring
- Topic operations (create, list, describe, delete)
- Producer operations (send messages)
- Consumer operations (read messages)
- Consumer group management
- Real-time monitoring and metrics

Installation:
pip install mcp kafka-python

Usage:
python kafka_mcp_server.py

Or with Claude Desktop config:
{
  "mcpServers": {
    "kafka": {
      "command": "python",
      "args": ["/path/to/kafka_mcp_server.py"],
      "env": {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"
      }
    }
  }
}
"""

import asyncio
import json
import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable
from kafka.structs import TopicPartition
from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094').split(','),
    'security_protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
    'sasl_mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl_username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl_password': os.getenv('KAFKA_SASL_PASSWORD'),
    'ssl_cafile': os.getenv('KAFKA_SSL_CAFILE'),
    'ssl_certfile': os.getenv('KAFKA_SSL_CERTFILE'),
    'ssl_keyfile': os.getenv('KAFKA_SSL_KEYFILE'),
}

# Remove None values from config
KAFKA_CONFIG = {k: v for k, v in KAFKA_CONFIG.items() if v is not None}

# Pydantic models for structured responses
class TopicInfo(BaseModel):
    """Topic information structure."""
    name: str
    partitions: int
    replication_factor: int
    configs: Dict[str, str] = Field(default_factory=dict)

class PartitionInfo(BaseModel):
    """Partition information structure."""
    partition: int
    leader: int
    replicas: List[int]
    isr: List[int]

class TopicDetails(BaseModel):
    """Detailed topic information."""
    name: str
    partitions: List[PartitionInfo]
    configs: Dict[str, str]

class ConsumerGroupInfo(BaseModel):
    """Consumer group information."""
    group_id: str
    state: str
    members: int
    coordinator: Optional[str] = None

class MessageInfo(BaseModel):
    """Kafka message information."""
    topic: str
    partition: int
    offset: int
    key: Optional[str] = None
    value: str
    timestamp: int
    headers: Dict[str, str] = Field(default_factory=dict)

class ClusterInfo(BaseModel):
    """Kafka cluster information."""
    cluster_id: str
    brokers: List[Dict[str, Any]]
    topics_count: int
    partitions_count: int
    controller: Optional[Dict[str, Any]] = None

@dataclass
class KafkaContext:
    """Kafka connection context."""
    admin_client: KafkaAdminClient
    producer: KafkaProducer
    
    async def cleanup(self):
        """Clean up Kafka connections."""
        try:
            if self.producer:
                self.producer.close()
            if self.admin_client:
                self.admin_client.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Global variable to hold the KafkaContext for resource/prompt access
_kafka_context: Optional[KafkaContext] = None

@asynccontextmanager
async def kafka_lifespan(server: FastMCP) -> AsyncIterator[KafkaContext]:
    """Manage Kafka connections lifecycle."""
    global _kafka_context
    logger.info("Initializing Kafka connections...")
    
    admin_client = None
    producer = None

    try:
        # Create admin client
        admin_client = KafkaAdminClient(**KAFKA_CONFIG)
        
        # Create producer
        producer_config = KAFKA_CONFIG.copy()
        producer_config.update({
            'value_serializer': lambda v: json.dumps(v).encode('utf-8') if isinstance(v, (dict, list)) else str(v).encode('utf-8'),
            'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None, # Handle None key
        })
        producer = KafkaProducer(**producer_config)
        
        # Test connectivity
        try:
            metadata = admin_client.describe_cluster()
            logger.info(f"Connected to Kafka cluster: {metadata}")
        except NoBrokersAvailable as e:
            logger.error(f"No Kafka brokers available at {KAFKA_CONFIG.get('bootstrap_servers')}: {e}")
            raise
        except Exception as e:
            logger.warning(f"Could not get cluster metadata during init (this might be ok if brokers are starting): {e}")
        
        context = KafkaContext(admin_client=admin_client, producer=producer)
        _kafka_context = context # Store KafkaContext in global variable
        
        yield context
        
    except Exception as e:
        logger.error(f"Failed to initialize Kafka connections: {e}")
        # Clean up any partially created clients on failure
        if admin_client:
            try:
                admin_client.close()
            except Exception as close_e:
                logger.error(f"Error closing admin client during failed init: {close_e}")
        if producer:
            try:
                producer.close()
            except Exception as close_e:
                logger.error(f"Error closing producer during failed init: {close_e}")
        raise # Re-raise to indicate lifespan failure
    finally:
        logger.info("Cleaning up Kafka connections...")
        if _kafka_context: # Use global context for cleanup
            await _kafka_context.cleanup()
        _kafka_context = None # Clear global variable on cleanup

mcp = FastMCP("Kafka MCP Server", lifespan=kafka_lifespan)


@mcp.tool()
async def get_cluster_info(ctx: Context) -> ClusterInfo:
    """Get comprehensive Kafka cluster information."""
    kafka_ctx = ctx.request_context.lifespan_context
    admin = kafka_ctx.admin_client
    
    try:
        # Get cluster metadata
        metadata = admin.describe_cluster()
        
        cluster_id = 'unknown'
        brokers_data = []
        controller_data = None

        # Handle different metadata formats
        if isinstance(metadata, dict):
            cluster_id = metadata.get('cluster_id', 'unknown')
            brokers_data = metadata.get('brokers', [])
            controller_data = metadata.get('controller', None)
        elif hasattr(metadata, 'cluster_id'):
            cluster_id = metadata.cluster_id or "unknown"
            brokers_data = getattr(metadata, 'brokers', [])
            controller_data = getattr(metadata, 'controller', None)
        
        # Get all topics for counting
        topics_metadata = admin.list_topics()
        
        topics_count = 0
        partitions_count = 0

        if isinstance(topics_metadata, dict):
            topics_count = len(topics_metadata)
            for topic_name, topic_meta in topics_metadata.items():
                if hasattr(topic_meta, 'partitions') and topic_meta.partitions:
                    partitions_count += len(topic_meta.partitions)
                elif isinstance(topic_meta, dict) and 'partitions' in topic_meta and topic_meta['partitions']:
                    partitions_count += len(topic_meta['partitions'])
        else: # Likely a list of TopicMetadata objects directly
            topics_count = len(topics_metadata)
            for topic_meta in topics_metadata:
                if hasattr(topic_meta, 'partitions') and topic_meta.partitions:
                    partitions_count += len(topic_meta.partitions)
                elif isinstance(topic_meta, dict) and 'partitions' in topic_meta and topic_meta['partitions']:
                    partitions_count += len(topic_meta['partitions'])

        # Get broker information
        brokers = []
        for broker in brokers_data:
            brokers.append({
                "id": getattr(broker, 'nodeId', None) or broker.get('id', 'N/A'),
                "host": getattr(broker, 'host', None) or broker.get('host', 'N/A'),
                "port": getattr(broker, 'port', None) or broker.get('port', 'N/A'),
                "rack": getattr(broker, 'rack', None) or broker.get('rack', None)
            })
        
        # Get controller info
        controller = None
        if controller_data:
            controller = {
                "id": getattr(controller_data, 'nodeId', None) or controller_data.get('id', 'N/A'),
                "host": getattr(controller_data, 'host', None) or controller_data.get('host', 'N/A'),
                "port": getattr(controller_data, 'port', None) or controller_data.get('port', 'N/A')
            }
        
        return ClusterInfo(
            cluster_id=cluster_id,
            brokers=brokers,
            topics_count=topics_count,
            partitions_count=partitions_count,
            controller=controller
        )
        
    except Exception as e:
        raise Exception(f"Failed to get cluster info: {str(e)}")

# TOPIC OPERATIONS
@mcp.tool()
async def list_topics(ctx: Context, include_internal: bool = False) -> str:
    """List all topics in the Kafka cluster."""
    kafka_ctx = ctx.request_context.lifespan_context
    admin = kafka_ctx.admin_client
    
    try:
        topics_metadata = admin.list_topics()
        topics = []
        
        # Handle different return types from list_topics()
        topics_dict = {}
        if hasattr(topics_metadata, 'topics'):
            # ClusterMetadata object with topics attribute
            topics_dict = topics_metadata.topics
        elif isinstance(topics_metadata, dict):
            # Direct dict of topic metadata
            topics_dict = topics_metadata
        else:
            # Try iterating directly
            try:
                for topic in topics_metadata:
                    topic_name = None
                    if hasattr(topic, 'topic'):
                        topic_name = topic.topic
                    elif hasattr(topic, 'name'):
                        topic_name = topic.name
                    elif isinstance(topic, str):
                        topic_name = topic
                    if topic_name:
                        topics_dict[topic_name] = topic
            except Exception as e:
                logger.error(f"Failed to parse topics_metadata: {e}")
                raise Exception(f"Unexpected topics_metadata format: {type(topics_metadata)}")
        
        for topic_name, topic_metadata in topics_dict.items():
            # Skip internal topics unless requested
            if not include_internal and topic_name.startswith('__'):
                continue
                
            # Extract partition and replication info from metadata
            num_partitions = 0
            replication_factor = 0
            
            # Handle different metadata structures
            partitions_dict = None
            if hasattr(topic_metadata, 'partitions'):
                partitions_dict = topic_metadata.partitions
            elif isinstance(topic_metadata, dict) and 'partitions' in topic_metadata:
                partitions_dict = topic_metadata['partitions']
            
            if partitions_dict:
                num_partitions = len(partitions_dict)
                if partitions_dict:
                    # Get first partition to determine replication factor
                    first_partition = next(iter(partitions_dict.values()))
                    if hasattr(first_partition, 'replicas'):
                        replication_factor = len(first_partition.replicas)
                    elif isinstance(first_partition, dict) and 'replicas' in first_partition:
                        replication_factor = len(first_partition['replicas'])
            
            # Create topic info as dict instead of TopicInfo object
            topic_info = {
                "name": topic_name,
                "partitions": num_partitions,
                "replication_factor": replication_factor,
                "configs": {}  # Add empty configs to match expected format
            }
            topics.append(topic_info)
        
        # Sort topics by name and return as JSON string
        sorted_topics = sorted(topics, key=lambda x: x["name"])
        return json.dumps(sorted_topics, indent=2)
        
    except Exception as e:
        logger.error(f"Failed to list topics: {str(e)}")
        # Return error in the expected format
        error_response = {
            "error": f"Failed to list topics: {str(e)}",
            "topics": []
        }
        return json.dumps(error_response, indent=2)

@mcp.tool()
async def describe_topic(ctx: Context, topic_name: str) -> str:
    """Get detailed information about a specific topic."""
    kafka_ctx = ctx.request_context.lifespan_context
    admin = kafka_ctx.admin_client
    
    try:
        # Use the simple describe_topics approach from reference implementation
        metadata = admin.describe_topics([topic_name])
        
        logger.info(f"Topic metadata response: {metadata}")
        if metadata is None or len(metadata) == 0:
            return json.dumps({
                "error": f"Topic '{topic_name}' not found",
                "name": topic_name,
                "partitions": [],
                "configs": {}
            }, indent=2)
        
        topic_metadata = metadata[0]
        partitions_info = []
        
        # Parse partition information
        partitions_data = topic_metadata.get("partitions", [])
        if not partitions_data and hasattr(topic_metadata, 'partitions'):
            partitions_data = topic_metadata.partitions
            
        for partition in partitions_data:
            partition_info = {
                "partition": partition.get("partition") if isinstance(partition, dict) else getattr(partition, 'partition', 0),
                "leader": partition.get("leader") if isinstance(partition, dict) else getattr(partition, 'leader', -1),
                "replicas": partition.get("replicas", []) if isinstance(partition, dict) else list(getattr(partition, 'replicas', [])),
                "isr": partition.get("isr", []) if isinstance(partition, dict) else list(getattr(partition, 'isr', []))
            }
            partitions_info.append(partition_info)
        
        # Get topic configurations (optional)
        topic_configs = {}
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs_result = admin.describe_configs([config_resource])
            
            if config_resource in configs_result:
                for config_name, config_entry in configs_result[config_resource].items():
                    topic_configs[config_name] = config_entry.value
        except Exception as config_error:
            logger.warning(f"Could not get topic configs for {topic_name}: {config_error}")
            topic_configs = {}
        
        # Create the result in the same format as before
        result = {
            "name": topic_name,
            "partitions": partitions_info,
            "configs": topic_configs
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        logger.error(f"Failed to describe topic '{topic_name}': {str(e)}")
        return json.dumps({
            "error": f"Failed to describe topic '{topic_name}': {str(e)}",
            "name": topic_name,
            "partitions": [],
            "configs": {}
        }, indent=2)

@mcp.tool()
async def create_topic(
    ctx: Context,
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    configs: Optional[Dict[str, str]] = None
) -> str:
    """Create a new Kafka topic."""
    kafka_ctx = ctx.request_context.lifespan_context
    admin = kafka_ctx.admin_client
    
    try:
        topic_configs = configs or {}
        
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=topic_configs
        )
        
        # Create the topic
        responses = admin.create_topics([new_topic], validate_only=False)
        
        # Handle different response types
        if isinstance(responses, dict):
            # Standard case: dict of {topic_name: future}
            for topic, future in responses.items():
                try:
                    result = future.result(timeout=10)
                    # Check if result has error_code attribute (newer kafka-python versions)
                    if hasattr(result, 'error_code') and result.error_code != 0:
                        if result.error_code == 36:  # TOPIC_ALREADY_EXISTS
                            return f"Topic '{topic_name}' already exists"
                        else:
                            raise Exception(f"Kafka error code {result.error_code}: {getattr(result, 'error_message', 'Unknown error')}")
                    return f"Topic '{topic_name}' created successfully with {num_partitions} partitions and replication factor {replication_factor}"
                except TopicAlreadyExistsError:
                    return f"Topic '{topic_name}' already exists"
                except Exception as e:
                    raise Exception(f"Failed to create topic '{topic_name}': {str(e)}")
        else:
            # Handle direct response objects (some kafka-python versions)
            # Check if it's a successful response
            if hasattr(responses, 'topic_errors'):
                for topic_error in responses.topic_errors:
                    if hasattr(topic_error, 'error_code'):
                        if topic_error.error_code == 0:
                            return f"Topic '{topic_name}' created successfully with {num_partitions} partitions and replication factor {replication_factor}"
                        elif topic_error.error_code == 36:  # TOPIC_ALREADY_EXISTS
                            return f"Topic '{topic_name}' already exists"
                        else:
                            error_msg = getattr(topic_error, 'error_message', f'Error code: {topic_error.error_code}')
                            raise Exception(f"Failed to create topic: {error_msg}")
            
            # If we can't parse the response, assume success if no exception was thrown
            # This is a fallback for when the response format is unexpected but no error occurred
            logger.warning(f"Unexpected response type from create_topics: {type(responses)}. Value: {responses}")
            return f"Topic '{topic_name}' created successfully with {num_partitions} partitions and replication factor {replication_factor}"
                
    except Exception as e:
        raise Exception(f"Failed to create topic: {str(e)}")

@mcp.tool()
async def delete_topic(ctx: Context, topic_name: str) -> str:
    """Delete a Kafka topic."""
    kafka_ctx = ctx.request_context.lifespan_context
    admin = kafka_ctx.admin_client
    
    try:
        # Delete the topic
        responses = admin.delete_topics([topic_name], timeout_ms=10000)
        
        # Handle different response types
        if isinstance(responses, dict):
            # Standard case: dict of {topic_name: future}
            for topic, future in responses.items():
                try:
                    result = future.result(timeout=10)
                    # Check if result has error_code attribute
                    if hasattr(result, 'error_code') and result.error_code != 0:
                        if result.error_code == 3:  # UNKNOWN_TOPIC_OR_PARTITION
                            return f"Topic '{topic_name}' does not exist or was already deleted"
                        else:
                            error_msg = getattr(result, 'error_message', f'Error code: {result.error_code}')
                            raise Exception(f"Failed to delete topic: {error_msg}")
                    return f"Topic '{topic_name}' deleted successfully"
                except Exception as e:
                    raise Exception(f"Failed to delete topic '{topic_name}': {str(e)}")
        else:
            # Handle direct response objects (some kafka-python versions)
            if hasattr(responses, 'topic_error_codes'):
                for topic_error in responses.topic_error_codes:
                    if hasattr(topic_error, 'error_code') or (isinstance(topic_error, tuple) and len(topic_error) >= 2):
                        # Handle both object and tuple formats
                        if isinstance(topic_error, tuple):
                            topic_name_resp, error_code = topic_error[0], topic_error[1]
                        else:
                            topic_name_resp = getattr(topic_error, 'topic', topic_name)
                            error_code = topic_error.error_code
                        
                        if error_code == 0:
                            return f"Topic '{topic_name}' deleted successfully"
                        elif error_code == 3:  # UNKNOWN_TOPIC_OR_PARTITION
                            return f"Topic '{topic_name}' does not exist or was already deleted"
                        else:
                            error_msg = getattr(topic_error, 'error_message', f'Error code: {error_code}') if not isinstance(topic_error, tuple) else f'Error code: {error_code}'
                            raise Exception(f"Failed to delete topic: {error_msg}")
            
            # If we can't parse the response, log it for debugging but don't fail
            logger.warning(f"Unexpected response type from delete_topics: {type(responses)}. Value: {responses}")
            return f"Topic deletion request submitted for '{topic_name}' (response format unexpected but no error detected)"
                
    except Exception as e:
        raise Exception(f"Failed to delete topic: {str(e)}")

# PRODUCER OPERATIONS

@mcp.tool()
async def send_message(
    ctx: Context,
    topic: str,
    message: Union[str, Dict[str, Any]],
    key: Optional[str] = None,
    partition: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None
) -> str:
    """Send a message to a Kafka topic."""
    kafka_ctx = ctx.request_context.lifespan_context
    producer = kafka_ctx.producer
    
    try:
        # Prepare headers
        kafka_headers = []
        if headers:
            kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
        
        # Send message
        future = producer.send(
            topic=topic,
            value=message,
            key=key,
            partition=partition,
            headers=kafka_headers
        )
        
        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        
        message_info = {
            "topic": record_metadata.topic,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "key": key,
            "value": json.dumps(message) if isinstance(message, (dict, list)) else str(message),
            "timestamp": record_metadata.timestamp,
            "headers": headers or {}
        }
        
        return json.dumps(message_info, indent=2)
        
    except Exception as e:
        raise Exception(f"Failed to send message to topic '{topic}': {str(e)}")

@mcp.tool()
async def send_batch_messages(
    ctx: Context,
    topic: str,
    messages: List[Dict[str, Any]],
    flush_timeout: int = 30
) -> str:
    """Send multiple messages to a Kafka topic in batch."""
    kafka_ctx = ctx.request_context.lifespan_context
    producer = kafka_ctx.producer
    
    try:
        futures = []
        
        # Send all messages
        for msg_data in messages:
            message = msg_data.get('message', msg_data.get('value'))
            key = msg_data.get('key')
            partition = msg_data.get('partition')
            headers = msg_data.get('headers', {})
            
            kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
            
            future = producer.send(
                topic=topic,
                value=message,
                key=key,
                partition=partition,
                headers=kafka_headers
            )
            futures.append((future, msg_data))
        
        # Flush and wait for all messages
        producer.flush(timeout=flush_timeout)
        
        results = []
        for future, msg_data in futures:
            try:
                record_metadata = future.get(timeout=1)
                results.append({
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                    "key": msg_data.get('key'),
                    "value": json.dumps(msg_data.get('message', msg_data.get('value'))) if isinstance(msg_data.get('message', msg_data.get('value')), (dict, list)) else str(msg_data.get('message', msg_data.get('value'))),
                    "timestamp": record_metadata.timestamp,
                    "headers": msg_data.get('headers', {})
                })
            except Exception as e:
                logger.error(f"Failed to send message in batch: {e}")
        
        return json.dumps(results, indent=2)
        
    except Exception as e:
        raise Exception(f"Failed to send batch messages to topic '{topic}': {str(e)}")

# CONSUMER OPERATIONS

@mcp.tool()
async def consume_messages(
    ctx: Context,
    topic: str,
    max_messages: int = 10,
    timeout_ms: int = 10000,
    from_beginning: bool = False,
    partition: Optional[int] = None
) -> str:
    """Consume messages from a Kafka topic."""
    
    group_id = f'mcp-consumer-{int(time.time() * 1000)}'

    try:
        consumer_config = KAFKA_CONFIG.copy()
        consumer_config.update({
            'group_id': group_id,
            'auto_offset_reset': 'earliest' if from_beginning else 'latest',
            'enable_auto_commit': False,
            'value_deserializer': lambda m: m.decode('utf-8') if m else None,
            'key_deserializer': lambda m: m.decode('utf-8') if m else None,
            'session_timeout_ms': 30000,
            'request_timeout_ms': 60000,
            'heartbeat_interval_ms': 10000,
            'max_poll_interval_ms': 300000,
            'connections_max_idle_ms': 540000,
            'retry_backoff_ms': 1000,
            'reconnect_backoff_ms': 1000,
            'reconnect_backoff_max_ms': 10000,
            'metadata_max_age_ms': 300000,
        })
        
        # Retry consumer creation with exponential backoff
        max_retries = 3
        retry_delay = 1
        
        consumer = None
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(**consumer_config)
                logger.info(f"Consumer created successfully on attempt {attempt + 1}")
                break
            except Exception as e:
                logger.warning(f"Consumer creation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise Exception(f"Failed to create consumer after {max_retries} attempts: {e}")
        
        if not consumer:
            raise Exception("Failed to create consumer")
        
        try:
            # Subscribe to topic or assign specific partition
            if partition is not None:
                topic_partition = TopicPartition(topic, partition)
                consumer.assign([topic_partition])
                if from_beginning:
                    consumer.seek_to_beginning(topic_partition)
                else:
                    # For specific partition, if not from beginning, seek to end
                    end_offsets = consumer.end_offsets([topic_partition])
                    consumer.seek(topic_partition, end_offsets.get(topic_partition, 0))
            else:
                consumer.subscribe([topic])
            
            messages = []
            
            # Allow more time for consumer to join group and get assignments
            logger.info(f"Waiting for consumer group coordination for topic {topic}")
            await asyncio.sleep(3)  # Increased wait time for coordinator
            
            start_time = time.time() * 1000
            
            while len(messages) < max_messages:
                # Check timeout
                current_time = time.time() * 1000
                if (current_time - start_time) > timeout_ms:
                    logger.info(f"Consume timeout reached for topic {topic}. Returning {len(messages)} messages.")
                    break
                
                try:
                    # poll returns {TopicPartition: [ConsumerRecord, ...]}
                    msg_pack = consumer.poll(timeout_ms=2000, max_records=max_messages - len(messages))
                    
                    if not msg_pack:
                        await asyncio.sleep(0.5)
                        continue
                    
                    for tp, msgs in msg_pack.items():
                        for msg in msgs:
                            headers = {}
                            if msg.headers:
                                try:
                                    headers = {k: v.decode('utf-8') for k, v in msg.headers}
                                except Exception as header_decode_e:
                                    logger.warning(f"Failed to decode header: {header_decode_e}")
                                    headers = {k: str(v) for k, v in msg.headers}
                            
                            try:
                                message_info = {
                                    "topic": str(msg.topic),
                                    "partition": int(msg.partition),
                                    "offset": int(msg.offset),
                                    "key": str(msg.key) if msg.key is not None else None,
                                    "value": str(msg.value) if msg.value is not None else "",
                                    "timestamp": int(msg.timestamp) if msg.timestamp is not None else 0,
                                    "headers": headers
                                }
                                messages.append(message_info)
                            except (ValueError, TypeError) as conversion_error:
                                logger.warning(f"Failed to convert message data: {conversion_error}")
                                messages.append({
                                    "topic": str(getattr(msg, 'topic', 'unknown')),
                                    "partition": int(getattr(msg, 'partition', 0)),
                                    "offset": int(getattr(msg, 'offset', 0)),
                                    "key": str(getattr(msg, 'key', None)) if getattr(msg, 'key', None) is not None else None,
                                    "value": str(getattr(msg, 'value', '')),
                                    "timestamp": int(getattr(msg, 'timestamp', 0)),
                                    "headers": headers
                                })
                                
                except Exception as poll_error:
                    logger.warning(f"Poll error for topic {topic}: {poll_error}")
                    await asyncio.sleep(1)
                    continue
            
            if not messages:
                return json.dumps({
                    "message": f"No messages are currently available to consume from the \"{topic}\" topic. This may be because all messages have already been consumed by the current consumer group, or there are no new messages since the last fetch.",
                    "topic": topic,
                    "consumer_group": group_id,
                    "from_beginning": from_beginning,
                    "timeout_ms": timeout_ms,
                    "messages": []
                }, indent=2)
            
            return json.dumps(messages, indent=2)
            
        finally:
            consumer.close()
            
    except Exception as e:
        logger.error(f"Failed to consume messages from topic '{topic}': {str(e)}")
        raise Exception(f"Failed to consume messages from topic '{topic}': {str(e)}")

@mcp.tool()
async def get_topic_offsets(ctx: Context, topic: str) -> str:
    """Get earliest and latest offsets for all partitions of a topic."""
    
    try:
        consumer_config = KAFKA_CONFIG.copy()
        consumer_config.update({
            'group_id': f'mcp-offset-checker-{int(time.time() * 1000)}',
            'enable_auto_commit': False,
            'consumer_timeout_ms': 1000
        })
        
        consumer = KafkaConsumer(**consumer_config)
        
        try:
            # Get topic partitions
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                # Check if topic exists at all, if not, partitions_for_topic returns None
                kafka_ctx = ctx.request_context.lifespan_context
                admin = kafka_ctx.admin_client
                topics_meta = admin.list_topics()
                if isinstance(topics_meta, dict) and topic not in topics_meta:
                     raise Exception(f"Topic '{topic}' not found")
                elif not isinstance(topics_meta, dict):
                    # Handle case where topics_meta is not a dict
                    topic_names = []
                    if hasattr(topics_meta, 'topics'):
                        topic_names = list(topics_meta.topics.keys())
                    else:
                        for t in topics_meta:
                            if hasattr(t, 'topic'):
                                topic_names.append(t.topic)
                            elif hasattr(t, 'name'):
                                topic_names.append(t.name)
                    if topic not in topic_names:
                        raise Exception(f"Topic '{topic}' not found")
                
                raise Exception(f"Topic '{topic}' has no partitions or metadata not available yet.")
            
            topic_partitions = [TopicPartition(topic, p) for p in sorted(list(partitions))]
            
            # Get earliest offsets
            earliest_offsets = consumer.beginning_offsets(topic_partitions)
            
            # Get latest offsets
            latest_offsets = consumer.end_offsets(topic_partitions)
            
            result = {}
            for tp in topic_partitions:
                # Use .get with default 0 if partition not found, though it should be
                result[tp.partition] = {
                    'earliest': earliest_offsets.get(tp, 0),
                    'latest': latest_offsets.get(tp, 0),
                    'lag': latest_offsets.get(tp, 0) - earliest_offsets.get(tp, 0)
                }
            
            return json.dumps(result, indent=2)
            
        finally:
            consumer.close()
            
    except Exception as e:
        raise Exception(f"Failed to get offsets for topic '{topic}': {str(e)}")

# Helper for tools that might need an admin client but don't explicitly pass ctx.
# This ensures that an admin client is always available if the lifespan started successfully.
def admin_client_for_utility(ctx: Context) -> KafkaAdminClient:
    """Provides the admin client from the lifespan context."""
    kafka_ctx = ctx.request_context.lifespan_context
    if not kafka_ctx or not kafka_ctx.admin_client:
        raise RuntimeError("KafkaAdminClient not available. Server lifespan might have failed.")
    return kafka_ctx.admin_client

# CONSUMER GROUP OPERATIONS

@mcp.tool()
async def list_consumer_groups(ctx: Context) -> str:
    """List all consumer groups in the cluster."""
    admin = admin_client_for_utility(ctx)
    
    try:
        groups = admin.list_consumer_groups()
        
        result = []
        # Handle different return types from list_consumer_groups()
        for group in groups:
            coordinator_info = None
            group_id = None
            state = 'UNKNOWN'
            
            # Handle different group object types
            if hasattr(group, 'group_id'):
                # ConsumerGroupListing object
                group_id = group.group_id
                state = getattr(group, 'state', 'UNKNOWN')
                if hasattr(group, 'coordinator') and group.coordinator:
                    coordinator_info = f"{group.coordinator.host}:{group.coordinator.port}"
            elif isinstance(group, dict):
                # Dictionary format
                group_id = group.get('group_id')
                state = group.get('state', 'UNKNOWN')
                if 'coordinator' in group and group['coordinator']:
                    coordinator_info = f"{group['coordinator'].get('host')}:{group['coordinator'].get('port')}"
            elif isinstance(group, (tuple, list)) and len(group) >= 1:
                # Tuple/list format (group_id, ...)
                group_id = str(group[0])
                state = str(group[1]) if len(group) > 1 else 'UNKNOWN'
            else:
                # Fallback - try to convert to string
                group_id = str(group)
                state = 'UNKNOWN'
            
            if group_id:
                result.append({
                    "group_id": group_id,
                    "state": state,
                    "members": 0,  # Will be filled by describe operation if needed
                    "coordinator": coordinator_info
                })
        
        sorted_result = sorted(result, key=lambda x: x["group_id"])
        return json.dumps(sorted_result, indent=2)
        
    except Exception as e:
        raise Exception(f"Failed to list consumer groups: {str(e)}")

@mcp.tool()
async def describe_consumer_group(ctx: Context, group_id: str) -> str:
    """Get detailed information about a consumer group."""
    admin = admin_client_for_utility(ctx)
    
    try:
        # describe_consumer_groups returns a dict {group_id: ConsumerGroupDescription}
        group_descriptions = admin.describe_consumer_groups([group_id])
        
        if group_id not in group_descriptions:
            raise Exception(f"Consumer group '{group_id}' not found")
        
        group_desc = group_descriptions[group_id]
        
        members = []
        for member in group_desc.members:
            member_info = {
                'member_id': getattr(member, 'member_id', 'N/A'),
                'client_id': getattr(member, 'client_id', 'N/A'),
                'host': getattr(member, 'host', 'N/A'),
                'assignments': []
            }
            
            # Get topic partition assignments
            if hasattr(member, 'member_assignment') and member.member_assignment:
                # member_assignment.assignment is a list of TopicPartition objects
                for tp in getattr(member.member_assignment, 'assignment', []):
                    member_info['assignments'].append({
                        'topic': getattr(tp, 'topic', 'N/A'),
                        'partitions': list(getattr(tp, 'partitions', []))
                    })
            
            members.append(member_info)
        
        coordinator_info = None
        if hasattr(group_desc, 'coordinator') and group_desc.coordinator:
            coordinator_info = {
                "id": getattr(group_desc.coordinator, 'nodeId', None) or group_desc.coordinator.get('id', 'N/A'),
                "host": getattr(group_desc.coordinator, 'host', None) or group_desc.coordinator.get('host', 'N/A'),
                "port": getattr(group_desc.coordinator, 'port', None) or group_desc.coordinator.get('port', 'N/A')
            }

        result = {
            'group_id': group_id,
            'state': getattr(group_desc, 'state', 'UNKNOWN'),
            'protocol': getattr(group_desc, 'protocol', 'N/A'),
            'protocol_type': getattr(group_desc, 'protocol_type', 'N/A'),
            'members': members,
            'coordinator': coordinator_info
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        raise Exception(f"Failed to describe consumer group '{group_id}': {str(e)}")

# MONITORING AND UTILITIES

@mcp.tool()
async def get_topic_metrics(ctx: Context, topic: str) -> str:
    """Get comprehensive metrics for a topic."""
    # Combines topic info and offset data for complete metrics
    try:
        # Get basic topic info
        topic_details_str = await describe_topic(ctx, topic)
        topic_details = json.loads(topic_details_str)
        
        # Get offset information
        offsets_str = await get_topic_offsets(ctx, topic)
        offsets = json.loads(offsets_str)
        
        # Calculate summary metrics
        total_messages = sum(partition_offsets['lag'] for partition_offsets in offsets.values())
        total_partitions = len(offsets)
        
        # Determine replication factor robustly (might be 0 if no partitions)
        replication_factor = 0
        if topic_details.get('partitions') and len(topic_details['partitions']) > 0:
            # Assuming all partitions have the same replication factor
            # and topic_details['partitions'] is a list of partition info dicts
            if topic_details['partitions'][0].get('replicas'):
                replication_factor = len(topic_details['partitions'][0]['replicas'])
        
        result = {
            'topic_name': topic,
            'partitions_count': total_partitions,
            'replication_factor': replication_factor,
            'total_messages': total_messages,
            'partition_details': offsets,
            'configs': topic_details.get('configs', {})
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        raise Exception(f"Failed to get metrics for topic '{topic}': {str(e)}")

@mcp.tool()
async def health_check(ctx: Context) -> str:
    """Perform a comprehensive health check of the Kafka cluster."""
    # Tests multiple operations to verify cluster health
    try:
        start_time = time.time()
        
        # Test cluster connectivity
        cluster_info = await get_cluster_info(ctx)
        
        # Test basic operations
        topics_str = await list_topics(ctx, include_internal=True) # Include internal for a more robust check
        topics = json.loads(topics_str)
        
        consumer_groups_str = await list_consumer_groups(ctx)
        consumer_groups = json.loads(consumer_groups_str)
        
        response_time = (time.time() - start_time) * 1000
        
        result = {
            'status': 'healthy',
            'response_time_ms': round(response_time, 2),
            'cluster_id': cluster_info.cluster_id,
            'brokers_count': len(cluster_info.brokers),
            'topics_count': len(topics),
            'consumer_groups_count': len(consumer_groups),
            'timestamp': datetime.now().isoformat()
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        result = {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
        return json.dumps(result, indent=2)


@mcp.resource("kafka://cluster/info")
async def cluster_info_resource() -> str:
    """Get cluster information as a resource."""
    global _kafka_context
    if _kafka_context is None or _kafka_context.admin_client is None:
        return "Error: Kafka context not initialized or admin client not available. Server might not be running or failed to connect to Kafka."
    
    admin = _kafka_context.admin_client
    
    try:
        metadata = admin.describe_cluster()
        
        cluster_id = 'unknown'
        brokers_data = []
        controller_data = None

        if isinstance(metadata, dict):
            cluster_id = metadata.get('cluster_id', 'unknown')
            brokers_data = metadata.get('brokers', [])
            controller_data = metadata.get('controller', None)
        elif hasattr(metadata, 'cluster_id'):
            cluster_id = metadata.cluster_id or "unknown"
            brokers_data = getattr(metadata, 'brokers', [])
            controller_data = getattr(metadata, 'controller', None)
        else:
            logger.warning(f"Unexpected type for cluster metadata: {type(metadata)}")
            return "Error: Could not retrieve cluster info due to unexpected metadata format."

        topics_metadata = admin.list_topics()
        topics_count = 0
        partitions_count = 0

        if isinstance(topics_metadata, dict):
            topics_count = len(topics_metadata)
            for topic_name, topic_meta in topics_metadata.items():
                if hasattr(topic_meta, 'partitions') and topic_meta.partitions:
                    partitions_count += len(topic_meta.partitions)
                elif isinstance(topic_meta, dict) and 'partitions' in topic_meta and topic_meta['partitions']:
                    partitions_count += len(topic_meta['partitions'])
        else:
            topics_count = len(topics_metadata)
            for topic_meta in topics_metadata:
                if hasattr(topic_meta, 'partitions') and topic_meta.partitions:
                    partitions_count += len(topic_meta.partitions)
                elif isinstance(topic_meta, dict) and 'partitions' in topic_meta and topic_meta['partitions']:
                    partitions_count += len(topic_meta['partitions'])

        brokers_list = []
        for broker in brokers_data:
            brokers_list.append({
                "id": getattr(broker, 'nodeId', None) or broker.get('id', 'N/A'),
                "host": getattr(broker, 'host', None) or broker.get('host', 'N/A'),
                "port": getattr(broker, 'port', None) or broker.get('port', 'N/A'),
                "rack": getattr(broker, 'rack', None) or broker.get('rack', None)
            })
        
        controller_info = None
        if controller_data:
            controller_info = {
                "id": getattr(controller_data, 'nodeId', None) or controller_data.get('id', 'N/A'),
                "host": getattr(controller_data, 'host', None) or controller_data.get('host', 'N/A'),
                "port": getattr(controller_data, 'port', None) or controller_data.get('port', 'N/A')
            }
        
        return f"""
# Kafka Cluster Information

**Cluster ID:** {cluster_id}
**Brokers:** {len(brokers_list)}
**Topics:** {topics_count}
**Total Partitions:** {partitions_count}

## Brokers
{chr(10).join(f"- Broker {broker['id']}: {broker['host']}:{broker['port']}" for broker in brokers_list)}

## Controller
{f"Controller: Broker {controller_info['id']} ({controller_info['host']}:{controller_info['port']})" if controller_info else "Controller: Unknown"}
"""
    except Exception as e:
        return f"Error getting cluster info: {str(e)}"

@mcp.resource("kafka://topics/list")
async def topics_list_resource() -> str: # Removed ctx: Context
    """Get topics list as a resource."""
    global _kafka_context
    if _kafka_context is None or _kafka_context.admin_client is None:
        return "Error: Kafka context not initialized or admin client not available. Server might not be running or failed to connect to Kafka."

    admin = _kafka_context.admin_client
    
    try:
        topics_metadata = admin.list_topics()
        topics = []
        
        # Handle different formats returned by kafka-python versions
        if isinstance(topics_metadata, dict):
            topic_items = topics_metadata.items()
        else: # Assume it's an iterable of TopicMetadata objects
            topic_items = [(t.name, t) for t in topics_metadata]

        for topic_name, topic_metadata in topic_items:
            # Skip internal topics
            if topic_name.startswith('__'):
                continue
            
            num_partitions = 0
            replication_factor = 0
            

            partitions_dict = getattr(topic_metadata, 'partitions', None)
            if partitions_dict:
                num_partitions = len(partitions_dict)
                if partitions_dict:
                    first_partition_key = next(iter(partitions_dict))
                    replication_factor = len(getattr(partitions_dict[first_partition_key], 'replicas', []))
            elif isinstance(topic_metadata, dict) and 'partitions' in topic_metadata and topic_metadata['partitions']:
                partitions_dict = topic_metadata['partitions']
                num_partitions = len(partitions_dict)
                if partitions_dict:
                    first_partition_key = next(iter(partitions_dict))
                    replication_factor = len(partitions_dict[first_partition_key].get('replicas', []))
                
            topics.append({
                "name": topic_name,
                "partitions": num_partitions,
                "replication_factor": replication_factor
            })
        
        topics.sort(key=lambda x: x["name"])

        topics_text = []
        for topic in topics:
            topics_text.append(f"- **{topic['name']}** ({topic['partitions']} partitions, RF: {topic['replication_factor']})")
        
        return f"""
# Kafka Topics

Total Topics: {len(topics)}

{chr(10).join(topics_text)}
"""
    except Exception as e:
        return f"Error getting topics: {str(e)}"



@mcp.prompt()
def kafka_monitoring_prompt(topic_name: str = "") -> str:
    """Generate a prompt for Kafka monitoring and troubleshooting."""
    base_prompt = """You are a Kafka expert helping with cluster monitoring and troubleshooting. 

Please analyze the Kafka cluster state and provide insights on:
1. Cluster health and performance
2. Topic configuration and usage patterns
3. Consumer group performance
4. Potential issues or optimizations
5. Recommendations for improvements

Use the available Kafka MCP tools to gather comprehensive information."""
    
    if topic_name:
        return f"{base_prompt}\n\nFocus specifically on topic: {topic_name}"
    
    return base_prompt

@mcp.prompt()
def kafka_troubleshooting_prompt(issue_description: str) -> str:
    """Generate a prompt for Kafka troubleshooting."""
    return f"""You are a Kafka troubleshooting expert. A user is experiencing the following issue:

**Issue:** {issue_description}

Please help diagnose and resolve this issue by:
1. Using Kafka MCP tools to gather relevant information
2. Analyzing the cluster state and configuration
3. Identifying the root cause
4. Providing step-by-step resolution steps
5. Suggesting preventive measures

Use the available tools to inspect topics, consumer groups, cluster health, and other relevant aspects.
"""

if __name__ == "__main__":
    # Run the server
    mcp.run()
#!/usr/bin/env python3
"""
Kafka MCP Server Test Script
============================

This script tests and validates Kafka MCP server setup.

Usage:
    python test_kafka_mcp.py

Requirements:
    pip install mcp kafka-python

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    KAFKA_SECURITY_PROTOCOL=PLAINTEXT
    ... (other Kafka config vars)
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import Any

# Test if required packages are available
try:
    from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
    from mcp.client.stdio import stdio_client
    from mcp import ClientSession, StdioServerParameters
    from pydantic import AnyUrl # Added for resource testing
except ImportError as e:
    print(f"❌ Missing required package: {e}")
    print("Please install with: pip install mcp kafka-python")
    sys.exit(1)

class KafkaMCPTester:
    """Test suite for Kafka MCP Server."""
    
    def __init__(self):
        self.test_results = []
        self.test_topic = f"mcp-test-topic-{int(time.time())}"
        
    def log_test(self, test_name: str, success: bool, message: str = "", details: Any = None):
        """Log test results."""
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if message:
            print(f"    {message}")
        if details and not success:
            print(f"    Details: {details}")
        
        self.test_results.append({
            'test': test_name,
            'success': success,
            'message': message,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
    
    def print_summary(self):
        """Print test summary."""
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r['success'])
        failed_tests = total_tests - passed_tests
        
        print("\n" + "="*50)
        print("TEST SUMMARY")
        print("="*50)
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print("\nFailed Tests:")
            for result in self.test_results:
                if not result['success']:
                    print(f"  - {result['test']}: {result['message']}")
    
    def test_kafka_connectivity(self) -> bool:
        """Test direct Kafka connectivity."""
        print("\n🔌 Testing Kafka Connectivity...")
        
        try:
            # Get Kafka configuration
            kafka_config = {
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
                'security_protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
                'sasl_mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
                'sasl_username': os.getenv('KAFKA_SASL_USERNAME'),
                'sasl_password': os.getenv('KAFKA_SASL_PASSWORD'),
            }
            
            # Remove None values
            kafka_config = {k: v for k, v in kafka_config.items() if v is not None}
            
            # Create admin client
            admin = KafkaAdminClient(**kafka_config)
            
            # Test connectivity
            metadata = admin.describe_cluster()
            admin.close()
            
            cluster_id_str = "unknown"
            if isinstance(metadata, dict):
                cluster_id_str = metadata.get('cluster_id', 'unknown')
            elif hasattr(metadata, 'cluster_id'): # Fallback for object-like metadata
                cluster_id_str = metadata.cluster_id or "unknown"

            self.log_test(
                "Kafka Connectivity", 
                True, 
                f"Connected to cluster: {cluster_id_str}"
            )
            return True
            
        except Exception as e:
            self.log_test(
                "Kafka Connectivity", 
                False, 
                "Failed to connect to Kafka cluster", 
                str(e)
            )
            return False
    
    async def test_mcp_server_startup(self) -> bool:
        """Test if MCP server starts correctly."""
        print("\n🚀 Testing MCP Server Startup...")
        
        try:
            # Get the server script path
            server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
            
            if not os.path.exists(server_script):
                self.log_test(
                    "MCP Server File", 
                    False, 
                    f"Server script not found: {server_script}"
                )
                return False
            
            # Create server parameters
            server_params = StdioServerParameters(
                command="python",
                args=[server_script],
                env=dict(os.environ)
            )
            
            # Try to connect to the server
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    # Initialize the connection with timeout
                    # Increased timeout slightly for server to initialize Kafka clients
                    await asyncio.wait_for(session.initialize(), timeout=20.0) # Increased timeout
                    
                    self.log_test(
                        "MCP Server Startup", 
                        True, 
                        "Server started and initialized successfully"
                    )
                    return True
                    
        except asyncio.TimeoutError:
            self.log_test(
                "MCP Server Startup", 
                False, 
                "Server initialization timed out"
            )
            return False
        except Exception as e:
            self.log_test(
                "MCP Server Startup", 
                False, 
                "Failed to start MCP server", 
                str(e)
            )
            return False
    
    async def test_mcp_tools(self) -> bool:
        """Test MCP server tools."""
        print("\n🛠️ Testing MCP Tools...")
        
        try:
            server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
            server_params = StdioServerParameters(
                command="python",
                args=[server_script],
                env=dict(os.environ)
            )
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    
                    # Test list_tools
                    tools = await session.list_tools()
                    expected_tools = [
                        'get_cluster_info', 'list_topics', 'describe_topic', 
                        'create_topic', 'delete_topic', 'send_message',
                        'send_batch_messages', 
                        'consume_messages', 'get_topic_offsets', 
                        'list_consumer_groups', 'describe_consumer_group', 
                        'get_topic_metrics', 'health_check'
                    ]
                    
                    available_tools = [tool.name for tool in tools.tools]
                    missing_tools = [tool for tool in expected_tools if tool not in available_tools]
                    
                    if missing_tools:
                        self.log_test(
                            "MCP Tools Availability", 
                            False, 
                            f"Missing tools: {missing_tools}"
                        )
                        return False
                    
                    self.log_test(
                        "MCP Tools Availability", 
                        True, 
                        f"All {len(available_tools)} tools available"
                    )
                    
                    # Test health_check tool
                    try:
                        result = await session.call_tool("health_check", {})
                        
                        health_data_str = ""
                        if result.content and result.content[0].text:
                             health_data_str = result.content[0].text
                        else:
                            self.log_test(
                                "Health Check Tool", 
                                False, 
                                "Health check returned no content"
                            )
                            return False

                        health_data = json.loads(health_data_str)
                        
                        if health_data.get('status') == 'healthy':
                            self.log_test(
                                "Health Check Tool", 
                                True, 
                                f"Cluster healthy, response time: {health_data.get('response_time_ms', 'N/A')}ms"
                            )
                        else:
                            self.log_test(
                                "Health Check Tool", 
                                False, 
                                f"Cluster unhealthy: {health_data.get('error', 'Unknown error')}"
                            )
                            return False
                    except json.JSONDecodeError as e:
                        self.log_test(
                            "Health Check Tool", 
                            False, 
                            f"Failed to parse health check response (not JSON). Error: {str(e)}. Raw result: {health_data_str}" 
                        )
                        return False
                    except Exception as e:
                        self.log_test(
                            "Health Check Tool", 
                            False, 
                            f"Failed to execute health check. Error: {str(e)}. Raw result: {health_data_str}" 
                        )
                        return False
                    
                    return True
                    
        except Exception as e:
            self.log_test(
                "MCP Tools Test", 
                False, 
                "Failed to test MCP tools", 
                str(e)
            )
            return False
    
    async def test_topic_operations(self) -> bool:
        """Test topic creation, listing, and deletion."""
        print("\n📋 Testing Topic Operations...")
        
        try:
            server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
            server_params = StdioServerParameters(
                command="python",
                args=[server_script],
                env=dict(os.environ)
            )
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    
                    # Test topic creation
                    create_success = False
                    try:
                        result = await session.call_tool(
                            "create_topic", 
                            {
                                "topic_name": self.test_topic,
                                "num_partitions": 2,
                                "replication_factor": 1
                            }
                        )
                        
                        if result.content and result.content[0].text:
                            response_text = result.content[0].text
                            if "created successfully" in response_text or "already exists" in response_text:
                                self.log_test(
                                    "Topic Creation", 
                                    True, 
                                    f"Created/found test topic: {self.test_topic}"
                                )
                                create_success = True
                            else:
                                self.log_test(
                                    "Topic Creation", 
                                    False, 
                                    f"Unexpected response: {response_text}"
                                )
                        else:
                            self.log_test(
                                "Topic Creation", 
                                False, 
                                "Create topic returned no content"
                            )
                            
                    except Exception as e:
                        self.log_test(
                            "Topic Creation", 
                            False, 
                            "Failed to create topic", 
                            str(e)
                        )
                    
                    if not create_success:
                        return False # Cannot proceed without topic
                    
                    # Wait a moment for topic to be created/propagate
                    await asyncio.sleep(5)  # Increased wait time
                    
                    # Test topic listing
                    list_success = False
                    try:
                        result = await session.call_tool("list_topics", {"include_internal": True})
                        topics_data_str = ""
                        if result.content and result.content[0].text:
                            topics_data_str = result.content[0].text
                        else:
                            self.log_test(
                                "Topic Listing", 
                                False, 
                                "List topics returned no content"
                            )
                            return False

                        topics_data = json.loads(topics_data_str)
                        topic_names = [topic['name'] for topic in topics_data]
                        
                        if self.test_topic in topic_names:
                            self.log_test(
                                "Topic Listing", 
                                True, 
                                f"Found test topic in list of {len(topic_names)} topics"
                            )
                            list_success = True
                        else:
                            self.log_test(
                                "Topic Listing", 
                                False, 
                                f"Test topic '{self.test_topic}' not found in list: {topic_names}"
                            )
                            
                    except json.JSONDecodeError as e:
                        self.log_test(
                            "Topic Listing", 
                            False, 
                            f"Failed to parse topic list response (not JSON). Error: {str(e)}. Raw result: {topics_data_str}"
                        )
                    except Exception as e:
                        self.log_test(
                            "Topic Listing", 
                            False, 
                            f"Failed to list topics. Error: {str(e)}. Raw result: {topics_data_str}"
                        )
                    
                    if not list_success:
                        return False # Cannot proceed without listing topics
                    
                    # Wait a moment for topic metadata to be fully available
                    await asyncio.sleep(2)
                    
                    # Test topic description
                    describe_success = False
                    try:
                        result = await session.call_tool(
                            "describe_topic", 
                            {"topic_name": self.test_topic}
                        )
                        topic_details_str = ""
                        if result.content and result.content[0].text:
                            topic_details_str = result.content[0].text
                        else:
                            self.log_test(
                                "Topic Description", 
                                False, 
                                "Describe topic returned no content"
                            )
                            return False

                        topic_details = json.loads(topic_details_str)
                        
                        # Check if there's an error in the response
                        if 'error' in topic_details:
                            self.log_test(
                                "Topic Description", 
                                False, 
                                f"Error describing topic: {topic_details['error']}"
                            )
                        elif (topic_details['name'] == self.test_topic and 
                              len(topic_details['partitions']) == 2):
                            self.log_test(
                                "Topic Description", 
                                True, 
                                f"Topic has {len(topic_details['partitions'])} partitions as expected"
                            )
                            describe_success = True
                        else:
                            self.log_test(
                                "Topic Description", 
                                False, 
                                f"Unexpected topic structure: {topic_details}"
                            )
                            
                    except json.JSONDecodeError as e:
                        self.log_test(
                            "Topic Description", 
                            False, 
                            f"Failed to parse topic description response (not JSON). Error: {str(e)}. Raw result: {topic_details_str}"
                        )
                    except Exception as e:
                        self.log_test(
                            "Topic Description", 
                            False, 
                            f"Failed to describe topic. Error: {str(e)}. Raw result: {topic_details_str}"
                        )
                    
                    return create_success and list_success and describe_success
                    
        except Exception as e:
            self.log_test(
                "Topic Operations Test", 
                False, 
                "Failed to test topic operations", 
                str(e)
            )
            return False
    
    async def test_message_operations(self) -> bool:
        """Test sending and consuming messages."""
        print("\n💬 Testing Message Operations...")
        
        try:
            server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
            server_params = StdioServerParameters(
                command="python",
                args=[server_script],
                env=dict(os.environ)
            )
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    
                    # Test message sending
                    send_success = False
                    test_message = {"test": "message", "timestamp": int(time.time())}
                    test_key = "test-key"
                    
                    try:
                        result = await session.call_tool(
                            "send_message", 
                            {
                                "topic": self.test_topic,
                                "message": test_message,
                                "key": test_key
                            }
                        )
                        
                        message_info_str = ""
                        if result.content and result.content[0].text:
                            message_info_str = result.content[0].text
                        else:
                            self.log_test(
                                "Message Sending", 
                                False, 
                                "Send message returned no content"
                            )
                            return False

                        message_info = json.loads(message_info_str)
                        
                        if (message_info['topic'] == self.test_topic and 
                            message_info['key'] == test_key):
                            self.log_test(
                                "Message Sending", 
                                True, 
                                f"Message sent to partition {message_info['partition']}, offset {message_info['offset']}"
                            )
                            send_success = True
                        else:
                            self.log_test(
                                "Message Sending", 
                                False, 
                                f"Unexpected message info: {message_info}"
                            )
                            
                    except json.JSONDecodeError as e:
                        self.log_test(
                            "Message Sending", 
                            False, 
                            f"Failed to parse message sending response (not JSON). Error: {str(e)}. Raw result: {message_info_str}"
                        )
                    except Exception as e:
                        self.log_test(
                            "Message Sending", 
                            False, 
                            f"Failed to send message. Error: {str(e)}. Raw result: {message_info_str}"
                        )

                    if not send_success:
                        return False # Cannot proceed to consume if send fails
                    
                    # Wait a moment for message to be available
                    await asyncio.sleep(2)
                    
                    # Test message consumption
                    consume_success = False
                    try:
                        result = await session.call_tool(
                            "consume_messages", 
                            {
                                "topic": self.test_topic,
                                "max_messages": 5,
                                "from_beginning": True,
                                "timeout_ms": 10000
                            }
                        )
                        
                        messages_str = ""
                        if result.content and result.content[0].text:
                            messages_str = result.content[0].text
                        else:
                            self.log_test(
                                "Message Consumption", 
                                False, 
                                "Consume messages returned no content"
                            )
                            return False
                            
                        # Fix: The server returns a JSON array, not a single object
                        messages = json.loads(messages_str)
                        
                        # Ensure messages is a list
                        if not isinstance(messages, list):
                            # If it's a single message object, wrap it in a list
                            messages = [messages]
                        
                        if len(messages) > 0:
                            found_message = False
                            for msg in messages:
                                # Parse the JSON value if it's a JSON string
                                try:
                                    if isinstance(msg['value'], str) and msg['value'].strip().startswith(('{', '[')):
                                        deserialized_value = json.loads(msg['value'])
                                    else:
                                        deserialized_value = msg['value']
                                except (json.JSONDecodeError, KeyError):
                                    deserialized_value = msg.get('value', '')
                                
                                # Compare the message content
                                if (msg.get('key') == test_key and deserialized_value == test_message):
                                    found_message = True
                                    break
                            
                            if found_message:
                                self.log_test(
                                    "Message Consumption", 
                                    True, 
                                    f"Successfully consumed {len(messages)} messages, found test message"
                                )
                                consume_success = True
                            else:
                                self.log_test(
                                    "Message Consumption", 
                                    False, 
                                    f"Test message not found in consumed messages. Consumed: {messages}"
                                )
                        else:
                            self.log_test(
                                "Message Consumption", 
                                False, 
                                "No messages consumed"
                            )
                            
                    except json.JSONDecodeError as e:
                        self.log_test(
                            "Message Consumption", 
                            False, 
                            f"Failed to parse consumed messages (not JSON). Error: {str(e)}. Raw result: {messages_str}"
                        )
                    except Exception as e:
                        self.log_test(
                            "Message Consumption", 
                            False, 
                            f"Failed to consume messages. Error: {str(e)}. Raw result: {messages_str}"
                        )
                    
                    return send_success and consume_success
                    
        except Exception as e:
            self.log_test(
                "Message Operations Test", 
                False, 
                "Failed to test message operations", 
                str(e)
            )
            return False
    
    async def test_resources_and_prompts(self) -> bool:
        """Test MCP resources and prompts."""
        print("\n📚 Testing Resources and Prompts...")
        
        try:
            server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
            server_params = StdioServerParameters(
                command="python",
                args=[server_script],
                env=dict(os.environ)
            )
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    
                    # Test resources
                    resources_success = False
                    try:
                        resources = await session.list_resources()
                        expected_resource_uris = ['kafka://cluster/info', 'kafka://topics/list']
                        available_resource_uris = [str(r.uri) for r in resources.resources] # Convert AnyUrl to string
                        
                        missing_resources = [uri for uri in expected_resource_uris if uri not in available_resource_uris]
                        
                        if missing_resources:
                            self.log_test(
                                "Resources Availability", 
                                False, 
                                f"Missing resources: {missing_resources}. Available: {available_resource_uris}"
                            )
                        else:
                            self.log_test(
                                "Resources Availability", 
                                True, 
                                f"All {len(available_resource_uris)} resources available"
                            )
                            resources_success = True
                        
                        if not resources_success:
                            return False # Cannot proceed without listing resources
                        
                        # Test reading a resource
                        read_resource_success = False
                        resource_content_result = await session.read_resource(AnyUrl("kafka://cluster/info"))
                        
                        if resource_content_result.contents and resource_content_result.contents[0].text and len(resource_content_result.contents[0].text) > 0:
                            self.log_test(
                                "Resource Reading", 
                                True, 
                                "Successfully read cluster info resource"
                            )
                            read_resource_success = True
                        else:
                            self.log_test(
                                "Resource Reading", 
                                False,
                                "Empty resource content or invalid format"
                            )
                            
                    except Exception as e:
                        self.log_test(
                            "Resources Test", 
                            False, 
                            "Failed to test resources", 
                            str(e)
                        )
                        return False # Fail early if resource tests fail
                    
                    # Test prompts
                    prompts_success = False
                    try:
                        prompts = await session.list_prompts()
                        expected_prompts = ['kafka_monitoring_prompt', 'kafka_troubleshooting_prompt']
                        available_prompts = [p.name for p in prompts.prompts]
                        
                        missing_prompts = [p for p in expected_prompts if p not in available_prompts]
                        
                        if missing_prompts:
                            self.log_test(
                                "Prompts Availability", 
                                False, 
                                f"Missing prompts: {missing_prompts}"
                            )
                        else:
                            self.log_test(
                                "Prompts Availability", 
                                True, 
                                f"All {len(available_prompts)} prompts available"
                            )
                            prompts_success = True
                        
                        if not prompts_success:
                            return False # Cannot proceed without prompts
                        
                        # Test getting a prompt
                        prompt_execution_success = False
                        prompt_result = await session.get_prompt(
                            "kafka_monitoring_prompt", 
                            arguments={"topic_name": self.test_topic}
                        )
                        
                        if prompt_result.messages and len(prompt_result.messages) > 0:
                            self.log_test(
                                "Prompt Execution", 
                                True, 
                                "Successfully executed monitoring prompt"
                            )
                            prompt_execution_success = True
                        else:
                            self.log_test(
                                "Prompt Execution", 
                                False, 
                                "Empty prompt result"
                            )
                            
                    except Exception as e:
                        self.log_test(
                            "Prompts Test", 
                            False, 
                            "Failed to test prompts", 
                            str(e)
                        )
                        return False # Fail early if prompt tests fail
                    
                    return resources_success and read_resource_success and prompts_success and prompt_execution_success
                    
        except Exception as e:
            self.log_test(
                "Resources and Prompts Test", 
                False, 
                "Failed to test resources and prompts", 
                str(e)
            )
            return False
    
    async def cleanup_test_resources(self) -> bool:
        """Clean up test resources."""
        print("\n🧹 Cleaning up test resources...")
        
        try:
            server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
            server_params = StdioServerParameters(
                command="python",
                args=[server_script],
                env=dict(os.environ)
            )
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    
                    # Delete test topic
                    try:
                        result = await session.call_tool(
                            "delete_topic", 
                            {"topic_name": self.test_topic}
                        )
                        
                        if result.content and result.content[0].text and "deleted successfully" in result.content[0].text:
                            self.log_test(
                                "Cleanup", 
                                True, 
                                f"Deleted test topic: {self.test_topic}"
                            )
                        else:
                            self.log_test(
                                "Cleanup", 
                                False, 
                                f"Failed to delete topic: {result.content[0].text if result.content else 'No content'}. It might not have been created."
                            )
                            return False
                            
                    except Exception as e:
                        self.log_test(
                            "Cleanup", 
                            False, 
                            "Failed to delete test topic", 
                            str(e)
                        )
                        return False # Indicate cleanup failure
                    
                    return True
                    
        except Exception as e:
            self.log_test(
                "Cleanup", 
                False, 
                "Failed to cleanup test resources (client connection error)", 
                str(e)
            )
            return False
    
    async def run_all_tests(self) -> bool:
        """Run all tests in sequence."""
        print("🧪 Starting Kafka MCP Server Test Suite")
        print("=" * 50)
        
        # Test sequence
        # Cleanup should run last regardless of previous failures
        test_phases = [
            ("Kafka Connectivity", self.test_kafka_connectivity),
            ("MCP Server Startup", self.test_mcp_server_startup),
            ("MCP Tools", self.test_mcp_tools),
            ("Topic Operations", self.test_topic_operations),
            ("Message Operations", self.test_message_operations),
            ("Resources and Prompts", self.test_resources_and_prompts),
        ]
        
        overall_success = True
        
        for test_name, test_func in test_phases:
            try:
                if asyncio.iscoroutinefunction(test_func):
                    success = await test_func()
                else:
                    success = test_func()
                
                if not success:
                    overall_success = False

                    
            except Exception as e:
                self.log_test(
                    test_name, 
                    False, 
                    f"Test crashed: {str(e)}"
                )
                overall_success = False
        
        # Always attempt cleanup
        cleanup_succeeded = await self.cleanup_test_resources()
        if not cleanup_succeeded:
            overall_success = False
            
        return overall_success

def check_environment():
    """Check if environment is properly configured."""
    print("🔍 Checking Environment Configuration...")
    
    required_vars = ['KAFKA_BOOTSTRAP_SERVERS']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("\nPlease set the following environment variables:")
        print("export KAFKA_BOOTSTRAP_SERVERS=localhost:9092")
        print("export KAFKA_SECURITY_PROTOCOL=PLAINTEXT")
        print("\nOr create a .env file with these values.")
        return False
    
    print("✅ Environment configuration looks good")
    
    # Show current configuration
    print("\nCurrent Kafka Configuration:")
    kafka_vars = [var for var in os.environ.keys() if var.startswith('KAFKA_')]
    for var in sorted(kafka_vars):
        value = os.environ[var]
        # Hide sensitive values
        if 'PASSWORD' in var or 'SECRET' in var or 'USERNAME' in var: # Added username for completeness
            value = '*' * min(len(value), 8) # Show a few stars, not exact length
        print(f"  {var}={value}")
    
    return True

def main():
    """Main test function."""
    print("🚀 Kafka MCP Server Test Suite")
    print("=" * 50)
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Check if server file exists
    server_script = os.path.join(os.path.dirname(__file__), 'kafka_mcp_server.py')
    if not os.path.exists(server_script):
        print(f"❌ Server script not found: {server_script}")
        print("Please ensure kafka_mcp_server.py is in the same directory as this test script.")
        sys.exit(1)
    
    # Run tests
    tester = KafkaMCPTester()
    
    try:
        success = asyncio.run(tester.run_all_tests())
        tester.print_summary()
        
        if success:
            print("\n🎉 All tests passed! Your Kafka MCP server is ready to use.")
            print("\nNext steps:")
            print("1. Add the server to your Claude Desktop configuration")
            print("2. Restart Claude Desktop")
            print("3. Start asking Claude to help with your Kafka operations!")
            
            print("\nExample Claude queries to try:")
            print('• "Can you check the health of my Kafka cluster?"')
            print('• "List all topics and their partition counts"')
            print('• "Create a topic called \'user-events\' with 3 partitions"')
            print('• "Send a test message to the user-events topic"')
            print('• "Show me the last 10 messages from user-events"')
            
        else:
            print("\n❌ Some tests failed. Please review the errors above.")
            print("Common issues:")
            print("- Kafka cluster not running or not accessible")
            print("- Incorrect connection configuration")
            print("- Missing dependencies")
            print("- Network connectivity issues")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test suite crashed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
# Kafka MCP Server

A comprehensive Model Context Protocol (MCP) server for Apache Kafka operations, enabling seamless integration with Claude Desktop and other MCP clients.

## 🚀 Features

- **Cluster Management**: Monitor cluster health, broker information, and metadata
- **Topic Operations**: Create, list, describe, and delete Kafka topics
- **Message Operations**: Send and consume messages with flexible configuration
- **Consumer Group Management**: List and describe consumer groups
- **Real-time Monitoring**: Get topic metrics, offsets, and performance data
- **Health Checks**: Comprehensive cluster health monitoring
- **Secure Configuration**: Support for SASL, SSL, and various authentication methods

## 📋 Prerequisites

- Python 3.8+
- Apache Kafka cluster (local or remote)
- pip package manager

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/aswinayyolath/kafka-mcp-server.git
   cd kafka-mcp-server
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install mcp kafka-python
   ```

4. **Configure environment variables**:
   ```bash
   cp .env.template .env
   # Edit .env with your Kafka configuration
   ```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file based on `.env.template`:

```bash
# Basic Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# SASL Configuration (if needed)
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=your_username
KAFKA_SASL_PASSWORD=your_password

# SSL Configuration (if needed)
KAFKA_SSL_CAFILE=/path/to/ca.pem
KAFKA_SSL_CERTFILE=/path/to/cert.pem
KAFKA_SSL_KEYFILE=/path/to/key.pem
```

### Docker Setup (Optional)

Start a local Kafka cluster using Docker:

```bash
docker-compose up -d
```

This will start Kafka on `localhost:9092`.

## 🧪 Testing

Run the comprehensive test suite to validate your setup:

```bash
# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Run tests
python test_kafka_mcp.py
```

The test suite validates:
- Kafka connectivity
- MCP server startup
- All available tools
- Topic operations
- Message operations
- Resources and prompts

## 🔧 Usage

### Standalone Server

Run the MCP server directly:

```bash
python kafka_mcp_server.py
```

### Claude Desktop Integration

Add to your Claude Desktop configuration (`claude_desktop_config.json`):

```json
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
```

## 🛠️ Available Tools

### Cluster Operations
- `get_cluster_info` - Get comprehensive cluster information
- `health_check` - Perform cluster health check

### Topic Operations
- `list_topics` - List all topics in the cluster
- `create_topic` - Create a new topic
- `describe_topic` - Get detailed topic information
- `delete_topic` - Delete a topic
- `get_topic_metrics` - Get comprehensive topic metrics
- `get_topic_offsets` - Get partition offsets

### Message Operations
- `send_message` - Send a single message
- `send_batch_messages` - Send multiple messages
- `consume_messages` - Consume messages from a topic

### Consumer Group Operations
- `list_consumer_groups` - List all consumer groups
- `describe_consumer_group` - Get detailed consumer group information

## 📚 Resources

The server provides MCP resources for easy access to cluster information:

- `kafka://cluster/info` - Real-time cluster information
- `kafka://topics/list` - Current topics list

## 🎯 Prompts

Built-in prompts for common scenarios:

- `kafka_monitoring_prompt` - Generate monitoring and troubleshooting guidance
- `kafka_troubleshooting_prompt` - Get help with specific issues

## 💡 Example Usage with Claude

Once integrated with Claude Desktop, you can ask:

- "Can you check the health of my Kafka cluster?"
- "List all topics and their partition counts"
- "Create a topic called 'user-events' with 3 partitions"
- "Send a test message to the user-events topic"
- "Show me the last 10 messages from user-events"
- "What consumer groups are active?"

## 🔒 Security

### Authentication

The server supports various Kafka authentication methods:

- **PLAINTEXT**: No authentication (development only)
- **SASL_PLAINTEXT**: SASL authentication over plain connection
- **SASL_SSL**: SASL authentication over SSL
- **SSL**: SSL client certificate authentication

### Best Practices

1. **Never commit `.env` files** - Use `.env.template` for examples
2. **Use SSL in production** - Always encrypt connections to production clusters
3. **Limit permissions** - Use dedicated service accounts with minimal required permissions
4. **Monitor access** - Log and monitor MCP server usage

## 🐛 Troubleshooting

### Common Issues

1. **Connection refused**:
   - Verify Kafka is running
   - Check `KAFKA_BOOTSTRAP_SERVERS` configuration
   - Ensure network connectivity

2. **Authentication failures**:
   - Verify SASL credentials
   - Check SSL certificate paths
   - Validate security protocol settings

3. **Topic not found**:
   - Ensure topic exists
   - Check topic name spelling
   - Verify permissions

### Debug Mode

Enable debug logging by setting:

```bash
export PYTHONPATH=.
python -c "import logging; logging.basicConfig(level=logging.DEBUG)"
python kafka_mcp_server.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes
4. Run tests: `python test_kafka_mcp.py`
5. Commit changes: `git commit -am 'Add feature'`
6. Push to branch: `git push origin feature-name`
7. Submit a pull request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Model Context Protocol](https://github.com/modelcontextprotocol) - For the MCP specification
- [Apache Kafka](https://kafka.apache.org/) - For the distributed streaming platform
- [kafka-python](https://github.com/dpkp/kafka-python) - For the Python Kafka client

## 📞 Support

For issues and questions:

1. Check the [troubleshooting section](#-troubleshooting)
2. Run the test suite to validate your setup
3. Review Kafka and MCP documentation
4. Open an issue with detailed error information

---

**Happy Kafka streaming with MCP! 🎉**

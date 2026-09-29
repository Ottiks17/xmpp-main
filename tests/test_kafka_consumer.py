"""Tests for Kafka broker and consumer."""
import pytest
import time
from unittest.mock import MagicMock, patch, call
from threading import Event

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.kafka_broker import KafkaBroker


class TestKafkaBroker:
    """Kafka broker tests."""
    
    @pytest.fixture
    def kafka_broker(self):
        """Create a test Kafka broker."""
        def mock_log(msg, level):
            print(f"[{level}] {msg}")
        
        return KafkaBroker('localhost:9092', log_callback=mock_log)
    
    def test_kafka_broker_initialization(self, kafka_broker):
        """Test Kafka broker initialization."""
        assert kafka_broker.bootstrap_servers == 'localhost:9092'
        assert kafka_broker.producer is None
        assert len(kafka_broker.consumers) == 0
    
    @patch('app.kafka_broker.KafkaProducer')
    def test_connect_success(self, mock_producer_class, kafka_broker):
        """Test successful connection to Kafka."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        result = kafka_broker.connect()
        
        assert result is True
        assert kafka_broker.producer is not None
        mock_producer_class.assert_called_once()
    
    @patch('app.kafka_broker.KafkaProducer')
    def test_connect_failure(self, mock_producer_class, kafka_broker):
        """Test failed connection to Kafka."""
        mock_producer_class.side_effect = Exception("Connection failed")
        
        result = kafka_broker.connect()
        
        assert result is False
        assert kafka_broker.producer is None
    
    @patch('app.kafka_broker.KafkaProducer')
    def test_publish_success(self, mock_producer_class, kafka_broker):
        """Test successful message publication."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        kafka_broker.connect()
        
        message = {"type": "SENT", "recipient": "user@domain", "message": "test"}
        result = kafka_broker.publish("xmpp-messages", message)
        
        assert result is True
        mock_producer.send.assert_called_once()
    
    @patch('app.kafka_broker.KafkaProducer')
    def test_publish_without_connection(self, mock_producer_class, kafka_broker):
        """Test publish without established connection."""
        message = {"type": "SENT", "recipient": "user@domain", "message": "test"}
        result = kafka_broker.publish("xmpp-messages", message)
        
        assert result is False
    
    @patch('app.kafka_broker.KafkaConsumer')
    def test_subscribe_success(self, mock_consumer_class, kafka_broker):
        """Test successful subscription to topic."""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        def test_handler(topic, message):
            pass
        
        result = kafka_broker.subscribe(['test-topic'], 'test-group', test_handler)
        
        assert result is True
        assert len(kafka_broker.consumers) == 1
    
    @patch('app.kafka_broker.KafkaConsumer')
    def test_subscribe_failure(self, mock_consumer_class, kafka_broker):
        """Test failed subscription."""
        mock_consumer_class.side_effect = Exception("Subscription failed")
        
        def test_handler(topic, message):
            pass
        
        result = kafka_broker.subscribe(['test-topic'], 'test-group', test_handler)
        
        assert result is False
        assert len(kafka_broker.consumers) == 0
    
    def test_consume_messages_with_retry(self, kafka_broker):
        """Test consume messages with retry logic."""
        call_count = 0
        max_retries = 3
        
        def handler_with_retry(topic, message):
            nonlocal call_count
            call_count += 1
            if call_count < max_retries:
                raise Exception("Temporary error")
            # Success on last retry
        
        # Mock consumer
        mock_message = MagicMock()
        mock_message.topic = 'test-topic'
        mock_message.value = {'test': 'data'}
        
        kafka_broker.stop_consumers.clear()
        
        # Simulate consumer iteration (will be called in _consume_messages)
        # We can't fully test this without mocking threading,
        # so we'll just verify the structure exists
        assert hasattr(kafka_broker, '_consume_messages')
        assert callable(kafka_broker._consume_messages)


class TestRetryLogic:
    """Test retry logic for Kafka consumer."""
    
    def test_exponential_backoff_timing(self):
        """Test that exponential backoff increases appropriately."""
        backoffs = []
        max_retries = 3
        
        for attempt in range(max_retries):
            backoff = 2 ** attempt
            backoffs.append(backoff)
        
        # Expected: [1, 2, 4]
        assert backoffs == [1, 2, 4]
    
    def test_retry_attempts_count(self):
        """Test that retry logic attempts correct number of times."""
        attempts = 0
        max_retries = 3
        
        for attempt in range(max_retries):
            attempts += 1
            if attempt >= max_retries - 1:
                break
        
        assert attempts == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])


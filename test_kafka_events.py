import sys
import os
import uuid
import json
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.utils.kafka_producer import publish_analytics_event, publish_comment_event, publish_post_event


class MockPost:
    def __init__(self):
        self.id = uuid.UUID("7ef6f0d1-a2b7-4f18-94a2-987654321abc")
        self.post_code = "POST-100001"
        self.title = "Best Apartments in Kokapet"
        self.content = "Prestige High Fields offers excellent investment opportunities with premium amenities."
        self.location = "Kokapet, Hyderabad"
        self.thumbnail_url = "https://cdn.zpc.com/post/100001.jpg"
        self.status = "ACTIVE"


class MockComment:
    def __init__(self):
        self.id = uuid.UUID("11111111-2222-3333-4444-555555555555")
        self.comment_code = "COMM-100001"
        self.post_id = uuid.UUID("7ef6f0d1-a2b7-4f18-94a2-987654321abc")
        self.content = "There is a water leakage issue in Tower B."
        self.status = "ACTIVE"


def test_publish_post_created_event():
    post = MockPost()
    mock_producer = MagicMock()
    with patch("app.utils.kafka_producer.get_kafka_producer", return_value=mock_producer):
        success = publish_post_event("POST_CREATED", post)
        assert success is True
        assert mock_producer.send.called

        args, kwargs = mock_producer.send.call_args
        topic = args[0]
        event = kwargs["value"]

        assert topic == "post-events"
        assert event["eventType"] == "POST_CREATED"
        assert event["source"] == "post-service"
        assert event["payload"]["id"] == "7ef6f0d1-a2b7-4f18-94a2-987654321abc"
        assert event["payload"]["postCode"] == "POST-100001"
        assert event["payload"]["title"] == "Best Apartments in Kokapet"
        print("[PASSED] test_publish_post_created_event")
        print("Produced Post Event payload sample:")
        print(json.dumps(event, indent=2))


def test_publish_comment_created_event():
    comment = MockComment()
    mock_producer = MagicMock()
    with patch("app.utils.kafka_producer.get_kafka_producer", return_value=mock_producer):
        success = publish_comment_event("COMMENT_CREATED", comment, property_id="PROP-100001")
        assert success is True
        assert mock_producer.send.called

        args, kwargs = mock_producer.send.call_args
        topic = args[0]
        event = kwargs["value"]

        assert topic == "comments_events"
        assert event["eventType"] == "COMMENT_CREATED"
        assert event["source"] == "post-service"
        assert event["payload"]["id"] == "11111111-2222-3333-4444-555555555555"
        assert event["payload"]["commentId"] == "COMM-100001"
        assert event["payload"]["postId"] == "7ef6f0d1-a2b7-4f18-94a2-987654321abc"
        assert event["payload"]["propertyId"] == "PROP-100001"
        print("[PASSED] test_publish_comment_created_event")
        print("Produced Comment Event payload sample:")
        print(json.dumps(event, indent=2))


def test_publish_post_viewed_analytics_event():
    mock_producer = MagicMock()
    payload = {
        "userId": "USR-100001",
        "postId": "7ef6f0d1-a2b7-4f18-94a2-987654321abc",
        "postCode": "POST-100001",
        "createdBy": "USR-100020",
        "city": "Hyderabad",
        "visibility": "PUBLIC",
        "viewDuration": 48,
    }
    with patch("app.utils.kafka_producer.get_kafka_producer", return_value=mock_producer):
        success = publish_analytics_event("POST_VIEWED", payload, key=payload["postId"])
        assert success is True
        args, kwargs = mock_producer.send.call_args
        event = kwargs["value"]
        assert args[0] == "post-events"
        assert event["eventType"] == "POST_VIEWED"
        assert event["payload"]["postCode"] == "POST-100001"
        print("[PASSED] test_publish_post_viewed_analytics_event")
        print(json.dumps(event, indent=2))


def test_publish_comment_created_analytics_event():
    mock_producer = MagicMock()
    payload = {
        "userId": "USR-100001",
        "commentId": "COMM-100001",
        "postId": "POST-100001",
        "parentCommentId": None,
    }
    with patch("app.utils.kafka_producer.get_kafka_producer", return_value=mock_producer):
        success = publish_analytics_event(
            "COMMENT_CREATED",
            payload,
            key="COMM-100001",
            topic="comments_events",
        )
        assert success is True
        args, kwargs = mock_producer.send.call_args
        event = kwargs["value"]
        assert args[0] == "comments_events"
        assert event["eventType"] == "COMMENT_CREATED"
        assert event["payload"]["parentCommentId"] is None
        print("[PASSED] test_publish_comment_created_analytics_event")
        print(json.dumps(event, indent=2))


if __name__ == "__main__":
    test_publish_post_created_event()
    test_publish_comment_created_event()
    test_publish_post_viewed_analytics_event()
    test_publish_comment_created_analytics_event()

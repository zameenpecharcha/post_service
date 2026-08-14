import os
import json
import uuid
import re
import ssl
import tempfile
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
from app.utils.log_utils import log_msg

load_dotenv()

_producer = None


def _parse_jaas_config(jaas_config: str):
    username, password = None, None
    if not jaas_config:
        return username, password
    u_match = re.search(r'username="([^"]+)"', jaas_config)
    p_match = re.search(r'password="([^"]+)"', jaas_config)
    if u_match:
        username = u_match.group(1)
    if p_match:
        password = p_match.group(1)
    return username, password


def _prepare_ssl_cafile(ca_file: str):
    """Rewrite the CA as LF PEM so OpenSSL can load Docker/Windows copies."""
    if not ca_file or not os.path.isfile(ca_file):
        return None
    try:
        text = open(ca_file, "r", encoding="utf-8", errors="ignore").read()
    except OSError as e:
        log_msg("error", f"Cannot read Kafka CA file {ca_file}: {e}")
        return None
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    begin, end = "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----"
    if begin not in text or end not in text:
        log_msg("error", f"Kafka CA file is not a PEM certificate: {ca_file}")
        return None
    body = "".join(text.split(begin, 1)[1].split(end, 1)[0].split())
    lines = [body[i:i + 64] for i in range(0, len(body), 64)]
    pem = begin + "\n" + "\n".join(lines) + "\n" + end + "\n"
    fd, tmp_path = tempfile.mkstemp(suffix=".pem", prefix="kafka-ca-")
    os.close(fd)
    with open(tmp_path, "w", encoding="ascii", newline="\n") as handle:
        handle.write(pem)
    try:
        ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).load_verify_locations(cafile=tmp_path)
    except Exception as e:
        log_msg("error", f"Invalid Kafka CA PEM {ca_file}: {e}")
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return None
    return tmp_path


def _build_ssl_context(ca_file: str):
    prepared = _prepare_ssl_cafile(ca_file)
    if not prepared:
        return None
    ctx = ssl.create_default_context(cafile=prepared)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    except (AttributeError, ValueError):
        pass
    try:
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
    except ssl.SSLError:
        pass
    return ctx


def _reset_producer():
    global _producer
    if _producer is None:
        return
    try:
        _producer.close(timeout=1)
    except Exception:
        pass
    _producer = None


def get_kafka_producer():
    global _producer
    if _producer is not None:
        return _producer

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.getenv("SPRING_KAFKA_BOOTSTRAP_SERVERS", "zpc-kafka-zpc-f53a.i.aivencloud.com:27831")
    ) or ""
    for prefix in ("SASL_SSL://", "sasl_ssl://", "SSL://", "ssl://", "PLAINTEXT://"):
        bootstrap_servers = bootstrap_servers.replace(prefix, "")
    security_protocol = (os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL") or "SASL_SSL").strip()
    sasl_mechanism = (os.getenv("KAFKA_SASL_MECHANISM", "PLAIN") or "PLAIN").strip()
    jaas_config = os.getenv("KAFKA_SASL_JAAS_CONFIG", "")

    jaas_user, jaas_pass = _parse_jaas_config(jaas_config)
    sasl_username = (os.getenv("KAFKA_SASL_USERNAME", jaas_user or "avnadmin") or "avnadmin").strip()
    sasl_password = (os.getenv("KAFKA_SASL_PASSWORD", jaas_pass or "") or "").strip()

    if not bootstrap_servers:
        log_msg("error", "KAFKA_BOOTSTRAP_SERVERS is not set. KafkaProducer will not be initialized.")
        return None
    if security_protocol.upper() != "PLAINTEXT" and not sasl_password:
        log_msg("error", "KAFKA_SASL_PASSWORD is not set. KafkaProducer will not be initialized.")
        return None

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ca_file_default = os.path.join(base_dir, "config", "aiven-ca.pem")
    ca_file = os.getenv("KAFKA_SSL_CAFILE", ca_file_default)

    config = {
        "bootstrap_servers": [s.strip() for s in bootstrap_servers.split(",") if s.strip()],
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
        "client_id": "post-service",
        "acks": 1,
        "retries": 3,
        "retry_backoff_ms": 200,
        "request_timeout_ms": 20000,
        "max_block_ms": 15000,
        # Aiven requires SASL before Kafka APIs. Auto version-probe skips SASL and hangs 60s.
        "api_version": (2, 5, 0),
        "connections_max_idle_ms": 180000,
    }

    if security_protocol.upper() != "PLAINTEXT":
        config["security_protocol"] = security_protocol
        config["sasl_mechanism"] = sasl_mechanism
        config["sasl_plain_username"] = sasl_username
        config["sasl_plain_password"] = sasl_password
        ssl_context = _build_ssl_context(ca_file)
        if ssl_context:
            config["ssl_context"] = ssl_context
        else:
            log_msg("error", f"Valid Kafka CA not found at {ca_file}. SASL_SSL producer will fail.")

    try:
        _producer = KafkaProducer(**config)
        log_msg(
            "info",
            f"KafkaProducer initialized for post_service: {bootstrap_servers} "
            f"protocol={security_protocol} mechanism={sasl_mechanism} user={sasl_username}",
        )
    except Exception as e:
        log_msg("error", f"Failed to initialize KafkaProducer in post_service: {str(e)}")
        _producer = None

    return _producer


def publish_post_event(event_type: str, post, thumbnail_url: str = None, correlation_id: str = None):
    """
    Publish POST_CREATED, POST_UPDATED, or POST_DELETED event to `post-events` topic.
    """
    try:
        producer = get_kafka_producer()
        if not producer:
            log_msg("warning", f"KafkaProducer unavailable. Event {event_type} for post_id={getattr(post, 'id', None)} not sent.")
            return False

        topic = os.getenv("KAFKA_POST_EVENTS_TOPIC", "post-events")
        event_id = str(uuid.uuid4())
        corr_id = correlation_id if correlation_id else event_id
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        post_id_str = str(post.id) if getattr(post, "id", None) else ""
        post_code_str = getattr(post, "post_code", "") or f"POST-{post_id_str}"

        payload = {
            "id": post_id_str,
            "postCode": post_code_str,
            "title": getattr(post, "title", "") or "",
            "content": getattr(post, "content", "") or "",
            "location": getattr(post, "location", "") or "",
            "thumbnailUrl": thumbnail_url or getattr(post, "thumbnail_url", "") or "",
            "status": getattr(post, "status", "ACTIVE") or "ACTIVE",
        }

        event = {
            "eventId": event_id,
            "eventType": event_type,
            "eventVersion": "1.0",
            "occurredAt": now_iso,
            "source": "post-service",
            "correlationId": corr_id,
            "payload": payload,
        }

        producer.send(topic, key=post_id_str, value=event).get(timeout=15)
        log_msg("info", f"Successfully published event {event_type} (eventId={event_id}) to topic {topic} for post_id={post_id_str}")
        return True
    except Exception as e:
        _reset_producer()
        log_msg("error", f"Error publishing {event_type} event for post_id={getattr(post, 'id', None)}: {str(e)}")
        return False


def publish_comment_event(event_type: str, comment, property_id: str = None, correlation_id: str = None):
    """
    Publish COMMENT_CREATED, COMMENT_UPDATED, or COMMENT_DELETED event to `comments-events` topic.
    """
    try:
        producer = get_kafka_producer()
        if not producer:
            log_msg("warning", f"KafkaProducer unavailable. Event {event_type} for comment_id={getattr(comment, 'id', None)} not sent.")
            return False

        topic = os.getenv("KAFKA_COMMENTS_EVENTS_TOPIC", "comments-events")
        event_id = str(uuid.uuid4())
        corr_id = correlation_id if correlation_id else event_id
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        comm_id_str = str(comment.id) if getattr(comment, "id", None) else ""
        comm_code_str = getattr(comment, "comment_code", None) or f"COMM-{comm_id_str}"
        post_id_str = str(comment.post_id) if getattr(comment, "post_id", None) else ""

        parent_id = getattr(comment, "parent_comment_id", None)
        payload = {
            "id": comm_id_str,
            "commentId": comm_code_str,
            "postId": post_id_str,
            "propertyId": str(property_id) if property_id else "",
            "content": getattr(comment, "content", "") or "",
            "status": getattr(comment, "status", "ACTIVE") or "ACTIVE",
            "userId": str(getattr(comment, "user_id", "") or ""),
            "parentCommentId": str(parent_id) if parent_id else None,
        }

        event = {
            "eventId": event_id,
            "eventType": event_type,
            "eventVersion": "1.0",
            "occurredAt": now_iso,
            "source": "post-service",
            "correlationId": corr_id,
            "payload": payload,
        }

        producer.send(topic, key=comm_id_str, value=event).get(timeout=15)
        log_msg("info", f"Successfully published event {event_type} (eventId={event_id}) to topic {topic} for comment_id={comm_id_str}")
        return True
    except Exception as e:
        _reset_producer()
        log_msg("error", f"Error publishing {event_type} event for comment_id={getattr(comment, 'id', None)}: {str(e)}")
        return False


def publish_analytics_event(
    event_type: str,
    payload: dict,
    key: str = None,
    topic: str = None,
    source: str = "post-service",
    correlation_id: str = None,
):
    """Publish ClickHouse activity events to the given Kafka topic."""
    try:
        producer = get_kafka_producer()
        if not producer:
            log_msg("warning", f"KafkaProducer unavailable. Analytics event {event_type} not sent.")
            return False

        resolved_topic = topic or os.getenv("KAFKA_POST_EVENTS_TOPIC", "post-events")
        event_id = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        event_key = key or str((payload or {}).get("userId") or event_id)

        event = {
            "eventId": event_id,
            "eventType": event_type,
            "eventVersion": "1.0",
            "occurredAt": now_iso,
            "source": source,
            "correlationId": correlation_id or event_id,
            "payload": payload or {},
        }

        producer.send(resolved_topic, key=event_key, value=event).get(timeout=15)
        log_msg("info", f"Successfully published analytics event {event_type} (eventId={event_id}) to topic {resolved_topic}")
        return True
    except Exception as e:
        _reset_producer()
        log_msg("error", f"Error publishing analytics event {event_type}: {str(e)}")
        return False

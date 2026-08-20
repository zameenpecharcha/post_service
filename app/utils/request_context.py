from contextvars import ContextVar
from typing import Optional

import grpc

_user_id: ContextVar[Optional[str]] = ContextVar("zpc_user_id", default=None)
_correlation_id: ContextVar[Optional[str]] = ContextVar("zpc_correlation_id", default=None)


def get_user_id() -> Optional[str]:
    return _user_id.get()


def get_correlation_id() -> Optional[str]:
    return _correlation_id.get()


def set_user_id(value: Optional[str]) -> None:
    _user_id.set(value or None)


def set_correlation_id(value: Optional[str]) -> None:
    _correlation_id.set(value or None)


def reset_context() -> None:
    _user_id.set(None)
    _correlation_id.set(None)


def metadata_lookup(metadata: dict, *keys: str) -> Optional[str]:
    lowered = {}
    for key, value in (metadata or {}).items():
        if isinstance(key, bytes):
            key = key.decode("utf-8", "ignore")
        if isinstance(value, bytes):
            value = value.decode("utf-8", "ignore")
        lowered[str(key).lower()] = str(value).strip() if value is not None else ""
    for key in keys:
        value = lowered.get(key.lower())
        if value:
            return value
    return None


def wrap_rpc_handler(handler, correlation_id: Optional[str] = None, user_id: Optional[str] = None):
    if handler is None:
        return None

    def bind(behavior):
        if behavior is None:
            return None

        def wrapped(request, context):
            set_correlation_id(correlation_id)
            set_user_id(user_id)
            try:
                return behavior(request, context)
            finally:
                reset_context()

        return wrapped

    kwargs = {
        "request_deserializer": handler.request_deserializer,
        "response_serializer": handler.response_serializer,
    }
    if not handler.request_streaming and not handler.response_streaming:
        return grpc.unary_unary_rpc_method_handler(bind(handler.unary_unary), **kwargs)
    if handler.request_streaming and not handler.response_streaming:
        return grpc.stream_unary_rpc_method_handler(bind(handler.stream_unary), **kwargs)
    if not handler.request_streaming and handler.response_streaming:
        return grpc.unary_stream_rpc_method_handler(bind(handler.unary_stream), **kwargs)
    return grpc.stream_stream_rpc_method_handler(bind(handler.stream_stream), **kwargs)

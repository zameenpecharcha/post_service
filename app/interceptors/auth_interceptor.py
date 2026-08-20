import grpc
from ..utils.log_utils import log_msg
from ..utils.jwt_utils import verify_jwt_token
from ..utils.request_context import metadata_lookup, wrap_rpc_handler


class AuthServerInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata or ())
        correlation_id = metadata_lookup(metadata, "x-correlation-id", "x-request-id")
        user_id = metadata_lookup(metadata, "x-user-id")
        raw = metadata.get("authorization") or metadata.get("Authorization") or ""
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        token = str(raw).replace("Bearer ", "").strip()

        if not token:
            def deny_handler(request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing token")
            return wrap_rpc_handler(
                grpc.unary_unary_rpc_method_handler(deny_handler),
                correlation_id,
                user_id,
            )
        try:
            payload, error = verify_jwt_token(token)
            if error:
                def deny_handler(request, context):
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid token")
                return wrap_rpc_handler(
                    grpc.unary_unary_rpc_method_handler(deny_handler),
                    correlation_id,
                    user_id,
                )
            return wrap_rpc_handler(continuation(handler_call_details), correlation_id, user_id)

        except Exception as e:
            log_msg("error", f"Interceptor token validation failed: {str(e)}")

            def deny_handler(request, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Token validation failed")

            return wrap_rpc_handler(
                grpc.unary_unary_rpc_method_handler(deny_handler),
                correlation_id,
                user_id,
            )

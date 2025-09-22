import base64
import os
import re
from typing import Optional, Tuple
import mimetypes
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError


_DATA_URL_RE = re.compile(r"^data:(?P<content_type>[^;]+);base64,(?P<data>.+)$")


def _split_data_url(base64_string: str) -> Tuple[Optional[str], str]:
    match = _DATA_URL_RE.match(base64_string)
    if match:
        return match.group("content_type"), match.group("data")
    return None, base64_string


def _infer_extension_from_content_type(content_type: Optional[str]) -> str:
    mapping = {
        # Images
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/bmp": ".bmp",
        "image/tiff": ".tiff",
        # Videos
        "video/mp4": ".mp4",
        "video/webm": ".webm",
        "video/quicktime": ".mov",
        "video/x-msvideo": ".avi",
        "video/x-matroska": ".mkv",
        "video/mpeg": ".mpeg",
    }
    ct = (content_type or "").lower()
    if ct in mapping:
        return mapping[ct]
    # Fallback to mimetypes
    ext = mimetypes.guess_extension(ct) if ct else None
    return ext or ""


def _choose_file_name(file_name: Optional[str], content_type: Optional[str], fallback_prefix: str) -> str:
    if file_name and "." in file_name:
        return file_name
    extension = _infer_extension_from_content_type(content_type)
    if not file_name:
        file_name = f"{fallback_prefix}_{uuid4().hex}"
    if extension and not file_name.endswith(extension):
        return f"{file_name}{extension}"
    return file_name


def upload_base64_to_s3(*, base64_string: str, key: str, content_type: Optional[str] = None, acl: str = "public-read") -> Tuple[str, int]:
    bucket = os.getenv("S3_BUCKET_NAME") or os.getenv("AWS_S3_BUCKET") or "zpc-app"
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

    detected_content_type, raw_b64 = _split_data_url(base64_string)
    effective_content_type = content_type or detected_content_type or "application/octet-stream"

    binary_data = base64.b64decode(raw_b64)

    client_kwargs = {"region_name": region}
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs.update({
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        })
        if aws_session_token:
            client_kwargs["aws_session_token"] = aws_session_token

    s3 = boto3.client("s3", **client_kwargs)

    put_kwargs = {
        "Bucket": bucket,
        "Key": key,
        "Body": binary_data,
        "ContentType": effective_content_type,
    }
    try:
        if acl:
            s3.put_object(**put_kwargs, ACL=acl)
        else:
            s3.put_object(**put_kwargs)
    except ClientError as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if error_code == "AccessControlListNotSupported":
            s3.put_object(**put_kwargs)
        else:
            raise

    public_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    return public_url, len(binary_data)


def build_post_key(post_id: int, media_id: int, file_name: Optional[str], content_type: Optional[str]) -> str:
    # Choose a sensible fallback prefix based on media kind
    kind = (content_type or "").split("/")[0].lower()
    fallback = "video" if kind == "video" else ("image" if kind == "image" else "media")
    safe_name = _choose_file_name(file_name, content_type, fallback_prefix=fallback)
    return f"post/{post_id}/{media_id}/{safe_name}"


def upload_file_to_s3(*, file_path: str, key: str, content_type: Optional[str] = None) -> Tuple[str, int]:
    """
    Uploads a local file to S3.
    If file_path is an HTTP/HTTPS URL (already uploaded), skip re-upload and return it directly.
    """
    # If already an URL (we pre-uploaded from the UI), just persist it as-is
    if file_path.startswith("http://") or file_path.startswith("https://"):
        return file_path, 0

    bucket = os.getenv("S3_BUCKET_NAME") or os.getenv("AWS_S3_BUCKET") or "zpc-app"
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

    # Infer content type if not provided
    if not content_type:
        guessed, _ = mimetypes.guess_type(file_path)
        content_type = guessed or "application/octet-stream"

    client_kwargs = {"region_name": region}
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.getenv("AWS_SESSION_TOKEN")
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs.update({
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        })
        if aws_session_token:
            client_kwargs["aws_session_token"] = aws_session_token

    s3 = boto3.client("s3", **client_kwargs)

    extra_args = {"ContentType": content_type}
    size_bytes = os.path.getsize(file_path)

    try:
        s3.upload_file(Filename=file_path, Bucket=bucket, Key=key, ExtraArgs=extra_args)
    except ClientError as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if error_code == "AccessControlListNotSupported":
            # Retry without ACL handling (upload_file doesn't take ACL directly unless in ExtraArgs)
            s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)
        else:
            raise

    public_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    return public_url, size_bytes


# --- Helpers to generate pre-signed GET URLs for private objects ---

def _parse_s3_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse common S3 URL formats and return (bucket, region, key).
    Supports:
      - https://{bucket}.s3.{region}.amazonaws.com/{key}
      - https://s3.{region}.amazonaws.com/{bucket}/{key}
      - https://{bucket}.s3.amazonaws.com/{key}
    Returns (None, None, None) if parsing fails.
    """
    try:
        if not url:
            return None, None, None
        # Normalize
        url = url.strip()
        # Virtual-hosted–style: {bucket}.s3.{region}.amazonaws.com/{key}
        # or legacy without region: {bucket}.s3.amazonaws.com/{key}
        import re
        m = re.match(r"^https?://([^.]+)\.s3(?:\.([^.]+))?\.amazonaws\.com/(.+)$", url)
        if m:
            bucket = m.group(1)
            region = m.group(2) or (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1")
            key = m.group(3)
            return bucket, region, key
        # Path-style: s3.{region}.amazonaws.com/{bucket}/{key}
        m2 = re.match(r"^https?://s3\.([^.]+)\.amazonaws\.com/([^/]+)/(.+)$", url)
        if m2:
            region = m2.group(1)
            bucket = m2.group(2)
            key = m2.group(3)
            return bucket, region, key
    except Exception:
        pass
    return None, None, None


def generate_presigned_get_url_from_url(public_url: str, expires_in: int = 3600) -> Optional[str]:
    """
    Given a (non-public) S3 object URL, generate a pre-signed GET URL.
    Returns None if generation fails.
    """
    try:
        bucket, region, key = _parse_s3_url(public_url)
        if not bucket or not key:
            return None

        client_kwargs = {"region_name": region or (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1")}
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_session_token = os.getenv("AWS_SESSION_TOKEN")
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs.update({
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            })
            if aws_session_token:
                client_kwargs["aws_session_token"] = aws_session_token

        s3 = boto3.client("s3", **client_kwargs)
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expires_in
        )
        return url
    except Exception:
        return None

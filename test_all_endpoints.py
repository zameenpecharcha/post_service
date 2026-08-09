#!/usr/bin/env python3
"""Exercise post_service gRPC RPCs and verify post schema rows."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
import jwt
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent
USER_ROOT = ROOT.parent / "user_service"
AUTH_PRIVATE = ROOT.parent / "auth_service" / "config" / "private.pem"

sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
load_dotenv()

from app.proto_files import post_pb2, post_pb2_grpc  # noqa: E402

POST_TARGET = os.getenv("POST_SERVICE_TEST_URL", "localhost:50055")
USER_TARGET = os.getenv("USER_SERVICE_TEST_URL", "localhost:50053")

RUN_ID = int(time.time())
EMAIL_A = f"posttest_a_{RUN_ID}@example.com"
EMAIL_B = f"posttest_b_{RUN_ID}@example.com"
EMAIL_BUILDER = f"posttest_builder_{RUN_ID}@example.com"

results: list[tuple[str, str, str]] = []


def record(rpc: str, ok: bool, detail: str = ""):
    if detail.startswith("SKIP"):
        results.append((rpc, "SKIP", detail))
        print(f"  [SKIP] {rpc} — {detail[6:].lstrip('— ').lstrip('- ')}")
        return
    results.append((rpc, "PASS" if ok else "FAIL", detail))
    mark = "OK" if ok else "FAIL"
    suffix = f" — {detail}" if detail else ""
    print(f"  [{mark}] {rpc}{suffix}")


def make_token(user_id: str, email: str, role: str = "USER") -> str:
    with open(AUTH_PRIVATE, "r", encoding="utf-8") as f:
        private_key = f.read()
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "session_id": str(uuid.uuid4()),
        "iat": now,
        "nbf": now,
        "exp": now + timedelta(hours=1),
        "iss": "ZPC",
        "aud": "graphql-api",
    }
    return jwt.encode(payload, private_key, algorithm="RS256")


def create_user(user_id: str, email: str, role: str = "USER") -> bool:
    script = f"""
import grpc, sys
sys.path.insert(0, r"{USER_ROOT}")
from app.proto_files import user_pb2, user_pb2_grpc
stub = user_pb2_grpc.UserServiceStub(grpc.insecure_channel("{USER_TARGET}"))
r = stub.CreateUser(user_pb2.CreateUserRequest(
    id="{user_id}", first_name="Post", last_name="Tester",
    email="{email}", phone="", role="{role}", bio="post test",
))
print("OK" if r.success else "FAIL:" + r.message)
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(USER_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    out = (proc.stdout or "").strip()
    if proc.returncode != 0 or not out.startswith("OK"):
        print(proc.stderr)
        return False
    return True


def db_engine():
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5434")
    name = os.getenv("DB_NAME", "postgres")
    ssl = os.getenv("DB_SSLMODE", "")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    connect_args = {"sslmode": ssl} if ssl else {}
    return create_engine(url, connect_args=connect_args)


def call(stub, method, request, token: str):
    md = [("authorization", f"Bearer {token}")]
    return getattr(stub, method)(request, metadata=md)


def s3_configured() -> bool:
    return bool(os.getenv("AWS_ACCESS_KEY_ID", "").strip('"') and os.getenv("AWS_SECRET_ACCESS_KEY", "").strip('"'))


def write_test_image() -> str:
    """Minimal 1x1 PNG for S3 upload tests."""
    import base64

    data = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
    )
    fd, path = tempfile.mkstemp(suffix=".png", prefix="zpc_post_test_")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    return path


def test_post_media(stub, post_id: str, user_id: str, token: str) -> str | None:
    """AddPostMedia + DeletePostMedia when AWS credentials are configured."""
    if not s3_configured():
        record("AddPostMedia", False, "SKIP — AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set")
        record("DeletePostMedia", False, "SKIP — no media_id without AddPostMedia")
        return None

    image_path = write_test_image()
    media_id = None
    try:
        r = call(
            stub,
            "AddPostMedia",
            post_pb2.PostMediaRequest(
                post_id=post_id,
                uploaded_by=user_id,
                media=[
                    post_pb2.PostMediaUpload(
                        media_type="image",
                        media_order=1,
                        file_name="test.png",
                        content_type="image/png",
                        file_path=image_path,
                    )
                ],
            ),
            token,
        )
        if r.success and r.post and r.post.media:
            media_id = r.post.media[0].id
            url = r.post.media[0].media_url or ""
            record("AddPostMedia", True, f"url={url[:60]}..." if url else "uploaded")
        else:
            record("AddPostMedia", False, r.message)
    except grpc.RpcError as e:
        record("AddPostMedia", False, f"{e.code()} {e.details()}")
    finally:
        try:
            os.remove(image_path)
        except OSError:
            pass

    if media_id:
        try:
            r = call(stub, "DeletePostMedia", post_pb2.MediaIdRequest(media_id=media_id), token)
            record("DeletePostMedia", r.success, r.message)
        except grpc.RpcError as e:
            record("DeletePostMedia", False, str(e.details()))
    else:
        record("DeletePostMedia", False, "SKIP — AddPostMedia did not return media_id")

    return media_id


def verify_db(post_id: str, comment_id: str | None, share_id: str | None):
    print("\n=== Database verification (post schema) ===")
    with db_engine().connect() as conn:
        prow = conn.execute(
            text(
                """
                SELECT id::text, user_id::text, title, status, is_pinned, like_count, comment_count, share_count
                FROM "post".posts WHERE id = :pid
                """
            ),
            {"pid": post_id},
        ).fetchone()
        print(f"  post.posts: {dict(prow._mapping) if prow else None}")
        record("DB post.posts", prow is not None, prow.title if prow else "missing")

        if comment_id:
            crow = conn.execute(
                text('SELECT id::text, post_id::text, user_id::text, status FROM "post".comments WHERE id = :cid'),
                {"cid": comment_id},
            ).fetchone()
            print(f"  post.comments: {dict(crow._mapping) if crow else None}")
            record("DB post.comments", crow is not None)

        lcount = conn.execute(
            text('SELECT COUNT(*) FROM "post".post_likes WHERE post_id = :pid'),
            {"pid": post_id},
        ).scalar()
        print(f"  post.post_likes count: {lcount}")
        record("DB post.post_likes", lcount is not None)

        if share_id:
            srow = conn.execute(
                text('SELECT id::text, post_id::text, shared_by::text FROM "post".post_shares WHERE id = :sid'),
                {"sid": share_id},
            ).fetchone()
            print(f"  post.post_shares: {dict(srow._mapping) if srow else None}")
            record("DB post.post_shares", srow is not None)

        rcount = conn.execute(
            text("SELECT COUNT(*) FROM api_gateway.reports WHERE entity_id = :pid"),
            {"pid": post_id},
        ).scalar()
        print(f"  api_gateway.reports (for post): {rcount}")
        record("DB api_gateway.reports", int(rcount or 0) >= 1)


def ensure_post_schema():
    """Apply minimal column fixes when local DB predates canonical schema."""
    with db_engine().begin() as conn:
        conn.execute(
            text('ALTER TABLE "post".posts ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT FALSE')
        )
        conn.execute(text('ALTER TABLE "post".posts ADD COLUMN IF NOT EXISTS pinned_at TIMESTAMPTZ'))


def main():
    print(f"=== post_service endpoint test (run_id={RUN_ID}) ===\n")
    ensure_post_schema()

    user_a = str(uuid.uuid4())
    user_b = str(uuid.uuid4())
    builder_id = str(uuid.uuid4())
    property_id = str(uuid.uuid4())

    for label, uid, email, role in [
        ("user A", user_a, EMAIL_A, "USER"),
        ("user B", user_b, EMAIL_B, "USER"),
        ("builder", builder_id, EMAIL_BUILDER, "BUILDER"),
    ]:
        record(f"prereq CreateUser ({label})", create_user(uid, email, role))

    token_a = make_token(user_a, EMAIL_A, "USER")
    token_b = make_token(user_b, EMAIL_B, "USER")
    token_builder = make_token(builder_id, EMAIL_BUILDER, "BUILDER")

    ch = grpc.insecure_channel(POST_TARGET)
    stub = post_pb2_grpc.PostsServiceStub(ch)

    post_id = comment_id = reply_id = share_id = media_id = report_id = None

    # CreatePost
    try:
        r = call(
            stub,
            "CreatePost",
            post_pb2.PostCreateRequest(
                user_id=user_a,
                title=f"Test Post {RUN_ID}",
                content="Hello from post_service test",
                visibility="PUBLIC",
                type="TEXT",
                location="Hyderabad",
                latitude=17.4,
                longitude=78.5,
                status="PUBLISHED",
                currency="INR",
            ),
            token_a,
        )
        post_id = r.post.id if r.post else None
        record("CreatePost", r.success and bool(post_id), r.message)
    except grpc.RpcError as e:
        record("CreatePost", False, f"{e.code()} {e.details()}")

    if not post_id:
        print("Cannot continue without post_id")
        sys.exit(1)

    # Reads / lists
    for rpc, req in [
        ("GetPost", post_pb2.PostRequest(post_id=post_id)),
        ("GetPostsByUser", post_pb2.GetPostsByUserRequest(user_id=user_a, page=1, limit=10, viewer_user_id=user_b)),
        ("GetMyPosts", post_pb2.GetMyPostsRequest(user_id=user_a, page=1, limit=10)),
        ("GetPublicPosts", post_pb2.GetPublicPostsRequest(page=1, limit=10, viewer_user_id=user_b)),
        ("SearchPosts", post_pb2.SearchPostsRequest(query="post_service", page=1, limit=10, viewer_user_id=user_b)),
        ("TrendingPosts", post_pb2.TrendingPostsRequest(limit=5, viewer_user_id=user_b)),
        ("GetPropertyPosts", post_pb2.GetPropertyPostsRequest(property_id=property_id, page=1, limit=5, viewer_user_id=user_b)),
        ("GetBuilderPosts", post_pb2.GetBuilderPostsRequest(user_ids=[builder_id], page=1, limit=10, viewer_user_id=user_b)),
    ]:
        try:
            r = call(stub, rpc, req, token_a)
            ok = getattr(r, "success", True)
            extra = ""
            if hasattr(r, "posts"):
                extra = f"count={len(r.posts)}"
            elif hasattr(r, "post") and r.post:
                extra = f"id={r.post.id}"
            record(rpc, ok, extra or getattr(r, "message", ""))
        except grpc.RpcError as e:
            record(rpc, False, f"{e.code()} {e.details()}")

    # UpdatePost
    try:
        r = call(
            stub,
            "UpdatePost",
            post_pb2.PostUpdateRequest(post_id=post_id, title="Updated title", content="Updated content"),
            token_a,
        )
        record("UpdatePost", r.success and r.post.title == "Updated title")
    except grpc.RpcError as e:
        record("UpdatePost", False, str(e.details()))

    # Pin / unpin
    for rpc in ("PinPost", "UnpinPost"):
        try:
            r = call(stub, rpc, post_pb2.PostOwnerRequest(post_id=post_id, user_id=user_a), token_a)
            record(rpc, r.success, r.message)
        except grpc.RpcError as e:
            record(rpc, False, str(e.details()))

    # Likes
    try:
        r = call(stub, "LikePost", post_pb2.LikeRequest(post_id=post_id, user_id=user_b, reaction_type="LIKE"), token_b)
        record("LikePost", r.success, f"likes={r.post.like_count if r.post else 0}")
    except grpc.RpcError as e:
        record("LikePost", False, str(e.details()))

    try:
        r = call(
            stub,
            "CheckLikeStatus",
            post_pb2.CheckLikeStatusRequest(post_id=post_id, user_id=user_b),
            token_b,
        )
        record("CheckLikeStatus", r.success and r.is_liked)
    except grpc.RpcError as e:
        record("CheckLikeStatus", False, str(e.details()))

    try:
        r = call(stub, "GetPostLikes", post_pb2.GetPostLikesRequest(post_id=post_id, page=1, limit=10), token_a)
        record("GetPostLikes", r.success, f"likes={len(r.likes)}")
    except grpc.RpcError as e:
        record("GetPostLikes", False, str(e.details()))

    # Comments
    try:
        r = call(
            stub,
            "CreateComment",
            post_pb2.CommentCreateRequest(post_id=post_id, comment="Nice post!", user_id=user_b),
            token_b,
        )
        comment_id = r.comment.id if r.comment else None
        record("CreateComment", r.success and bool(comment_id))
    except grpc.RpcError as e:
        record("CreateComment", False, str(e.details()))

    if comment_id:
        try:
            r = call(stub, "GetComment", post_pb2.CommentRequest(comment_id=comment_id), token_a)
            record("GetComment", r.success and r.comment.id == comment_id)
        except grpc.RpcError as e:
            record("GetComment", False, str(e.details()))

        try:
            r = call(
                stub,
                "UpdateComment",
                post_pb2.CommentUpdateRequest(comment_id=comment_id, comment="Updated comment"),
                token_b,
            )
            record("UpdateComment", r.success)
        except grpc.RpcError as e:
            record("UpdateComment", False, str(e.details()))

        try:
            r = call(
                stub,
                "ReplyComment",
                post_pb2.CommentCreateRequest(
                    post_id=post_id, parent_comment_id=comment_id, comment="Reply!", user_id=user_a
                ),
                token_a,
            )
            reply_id = r.comment.id if r.comment else None
            record("ReplyComment", r.success and bool(reply_id))
        except grpc.RpcError as e:
            record("ReplyComment", False, str(e.details()))

        try:
            r = call(stub, "GetComments", post_pb2.GetCommentsRequest(post_id=post_id, page=1, limit=10), token_a)
            record("GetComments", r.success, f"count={len(r.comments)}")
        except grpc.RpcError as e:
            record("GetComments", False, str(e.details()))

        if reply_id:
            try:
                r = call(stub, "GetReplies", post_pb2.GetRepliesRequest(comment_id=comment_id, page=1, limit=10), token_a)
                record("GetReplies", r.success, f"count={len(r.comments)}")
            except grpc.RpcError as e:
                record("GetReplies", False, str(e.details()))

        try:
            r = call(
                stub,
                "LikeComment",
                post_pb2.CommentLikeRequest(comment_id=comment_id, user_id=user_a, reaction_type="LIKE"),
                token_a,
            )
            record("LikeComment", r.success)
        except grpc.RpcError as e:
            record("LikeComment", False, str(e.details()))

        try:
            r = call(
                stub,
                "UnlikeComment",
                post_pb2.CommentLikeRequest(comment_id=comment_id, user_id=user_a),
                token_a,
            )
            record("UnlikeComment", r.success)
        except grpc.RpcError as e:
            record("UnlikeComment", False, str(e.details()))

    # Share
    try:
        r = call(
            stub,
            "SharePost",
            post_pb2.SharePostRequest(
                post_id=post_id, shared_by=user_b, share_type="SHARE", caption="Check this", visibility="PUBLIC"
            ),
            token_b,
        )
        share_id = r.share.id if r.share else None
        record("SharePost", r.success and bool(share_id))
    except grpc.RpcError as e:
        record("SharePost", False, str(e.details()))

    try:
        r = call(
            stub,
            "GetSharedPosts",
            post_pb2.GetSharedPostsRequest(user_id=user_b, page=1, limit=10, viewer_user_id=user_a),
            token_b,
        )
        record("GetSharedPosts", r.success, f"count={len(r.shares)}")
    except grpc.RpcError as e:
        record("GetSharedPosts", False, str(e.details()))

    # Media (S3 upload when AWS credentials are in .env; post_service must be restarted after .env changes)
    test_post_media(stub, post_id, user_a, token_a)

    # Archive / restore
    try:
        r = call(stub, "ArchivePost", post_pb2.PostOwnerRequest(post_id=post_id, user_id=user_a), token_a)
        record("ArchivePost", r.success, r.message)
    except grpc.RpcError as e:
        record("ArchivePost", False, str(e.details()))

    try:
        r = call(stub, "RestoreArchivedPost", post_pb2.PostOwnerRequest(post_id=post_id, user_id=user_a), token_a)
        record("RestoreArchivedPost", r.success, r.message)
    except grpc.RpcError as e:
        record("RestoreArchivedPost", False, str(e.details()))

    # Reports
    try:
        r = call(
            stub,
            "ReportPost",
            post_pb2.ReportPostRequest(
                post_id=post_id,
                reported_by=user_b,
                reported_user_id=user_a,
                reason_code="SPAM",
                description="test report",
            ),
            token_b,
        )
        report_id = r.report.id if r.report else None
        record("ReportPost", r.success and bool(report_id))
    except grpc.RpcError as e:
        record("ReportPost", False, str(e.details()))

    if comment_id:
        try:
            r = call(
                stub,
                "ReportComment",
                post_pb2.ReportCommentRequest(
                    comment_id=comment_id,
                    reported_by=user_a,
                    reported_user_id=user_b,
                    reason_code="ABUSE",
                    description="test",
                ),
                token_a,
            )
            record("ReportComment", r.success)
        except grpc.RpcError as e:
            record("ReportComment", False, str(e.details()))

    try:
        r = call(
            stub,
            "ReportUser",
            post_pb2.ReportUserRequest(user_id=user_b, reported_by=user_a, reason_code="OTHER", description="test"),
            token_a,
        )
        record("ReportUser", r.success)
    except grpc.RpcError as e:
        record("ReportUser", False, str(e.details()))

    try:
        r = call(
            stub,
            "ReportProperty",
            post_pb2.ReportPropertyRequest(
                property_id=property_id,
                reported_by=user_a,
                reported_user_id=user_b,
                reason_code="MISLEADING",
                description="test",
            ),
            token_a,
        )
        record("ReportProperty", r.success)
    except grpc.RpcError as e:
        record("ReportProperty", False, str(e.details()))

    try:
        r = call(stub, "GetMyReports", post_pb2.GetMyReportsRequest(reported_by=user_b, page=1, limit=10), token_b)
        record("GetMyReports", r.success, f"count={len(r.reports)}")
    except grpc.RpcError as e:
        record("GetMyReports", False, str(e.details()))

    verify_db(post_id, comment_id, share_id)

    # Cleanup
    try:
        r = call(stub, "UnlikePost", post_pb2.LikeRequest(post_id=post_id, user_id=user_b), token_b)
        record("UnlikePost", r.success)
    except grpc.RpcError as e:
        record("UnlikePost", False, str(e.details()))

    if share_id:
        try:
            r = call(
                stub,
                "DeleteSharedPost",
                post_pb2.DeleteSharedPostRequest(share_id=share_id, user_id=user_b),
                token_b,
            )
            record("DeleteSharedPost", r.success)
        except grpc.RpcError as e:
            record("DeleteSharedPost", False, str(e.details()))

    if comment_id:
        try:
            r = call(stub, "DeleteComment", post_pb2.CommentRequest(comment_id=comment_id), token_b)
            record("DeleteComment", r.success)
        except grpc.RpcError as e:
            record("DeleteComment", False, str(e.details()))

    try:
        r = call(stub, "DeletePost", post_pb2.PostRequest(post_id=post_id), token_a)
        record("DeletePost", r.success, r.message)
    except grpc.RpcError as e:
        record("DeletePost", False, str(e.details()))

    passed = sum(1 for _, s, _ in results if s == "PASS")
    failed = sum(1 for _, s, _ in results if s == "FAIL")
    skipped = sum(1 for _, s, _ in results if s == "SKIP")
    print(f"\n=== Summary: {passed} passed, {failed} failed, {skipped} skipped / {len(results)} checks ===")
    if failed:
        print("\nFailures:")
        for rpc, status, detail in results:
            if status == "FAIL":
                print(f"  - {rpc}: {detail}")
        sys.exit(1)


if __name__ == "__main__":
    main()

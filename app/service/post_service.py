import grpc
import os
from concurrent import futures
from contextlib import contextmanager
from dotenv import load_dotenv

from ..proto_files import post_pb2, post_pb2_grpc
from ..repository.post_repository import PostRepository
from ..utils.db_connection import get_db_engine
from ..utils.schema_helpers import parse_uuid, uuid_str, normalize_media_type
from ..utils.s3_utils import build_post_key, upload_file_to_s3
from ..interceptors.auth_interceptor import AuthServerInterceptor
from sqlalchemy.orm import sessionmaker

load_dotenv()
SessionLocal = sessionmaker(bind=get_db_engine(), expire_on_commit=False)


class PostsService(post_pb2_grpc.PostsServiceServicer):
    def __init__(self):
        self._SessionLocal = SessionLocal

    @contextmanager
    def _session(self):
        db = self._SessionLocal()
        try:
            yield db, PostRepository(db)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _convert_timestamp(self, dt):
        return int(dt.timestamp()) if dt else 0

    def _convert_to_proto_media(self, media, post_id=None):
        def _get(field):
            if hasattr(media, field):
                return getattr(media, field)
            mapping = getattr(media, "_mapping", None)
            if mapping and field in mapping:
                return mapping[field]
            return None

        return post_pb2.PostMedia(
            id=uuid_str(_get("id")),
            post_id=uuid_str(post_id or _get("entity_id")),
            media_type=_get("media_type") or "",
            media_url=_get("file_url") or "",
            media_order=int(_get("display_order") or 0),
            media_size=int(_get("file_size") or 0),
            caption="",
            uploaded_at=self._convert_timestamp(_get("created_at")),
        )

    def _convert_to_proto_comment(self, comment, repository=None, include_replies: bool = True):
        like_count = comment.like_count
        if like_count is None and repository:
            like_count = repository.get_comment_like_count(comment.id)

        nested = []
        if include_replies:
            thread = getattr(comment, "_thread_replies", None)
            if thread is not None:
                nested = [
                    self._convert_to_proto_comment(r, repository, include_replies=True)
                    for r in thread
                ]
            else:
                active_replies = [
                    r
                    for r in (comment.replies or [])
                    if getattr(r, "deleted_at", None) is None
                    and (getattr(r, "status", None) or "ACTIVE") == "ACTIVE"
                ]
                active_replies.sort(key=lambda r: getattr(r, "created_at", None) or 0)
                nested = [
                    self._convert_to_proto_comment(r, repository, include_replies=True)
                    for r in active_replies
                ]

        return post_pb2.Comment(
            id=uuid_str(comment.id),
            post_id=uuid_str(comment.post_id),
            parent_comment_id=uuid_str(comment.parent_comment_id),
            comment=comment.content or "",
            user_id=uuid_str(comment.user_id),
            user_first_name="",
            user_last_name="",
            user_role="",
            status=comment.status or "ACTIVE",
            added_at=self._convert_timestamp(comment.created_at),
            commented_at=self._convert_timestamp(comment.created_at),
            replies=nested,
            like_count=int(like_count or 0),
            is_anonymous=bool(comment.is_anonymous),
            edited_at=self._convert_timestamp(comment.updated_at),
            reply_count=int(getattr(comment, "reply_count", 0) or len(nested)),
            report_count=int(getattr(comment, "report_count", 0) or 0),
        )

    def _convert_to_proto_post(
        self,
        post,
        liked_post_ids=None,
        include_comments=True,
        repository=None,
        include_media=True,
        media_rows=None,
        like_count=None,
        comment_count=None,
    ):
        if not post:
            return None

        repo = repository
        if media_rows is None and include_media and repo:
            try:
                media_rows = repo.get_post_media(post.id)
            except Exception:
                media_rows = []
        elif media_rows is None:
            media_rows = []

        liked_ids = liked_post_ids or set()
        comments = []
        if include_comments and getattr(post, "comments", None):
            try:
                comments = [self._convert_to_proto_comment(c, repo) for c in post.comments]
            except Exception:
                comments = []

        return post_pb2.Post(
            id=uuid_str(post.id),
            user_id=uuid_str(post.user_id),
            user_first_name="",
            user_last_name="",
            user_email="",
            user_phone="",
            user_role="",
            title=post.title or "",
            content=post.content or "",
            visibility=post.visibility or "PUBLIC",
            type=post.post_type or "TEXT",
            location=post.location or "",
            latitude=float(post.latitude) if post.latitude is not None else 0.0,
            longitude=float(post.longitude) if post.longitude is not None else 0.0,
            price=float(post.price) if post.price else 0.0,
            status=post.status or "DRAFT",
            created_at=self._convert_timestamp(post.created_at),
            media=[self._convert_to_proto_media(m, post_id=post.id) for m in (media_rows or [])],
            comments=comments,
            like_count=int(like_count if like_count is not None else (post.like_count or 0)),
            comment_count=int(comment_count if comment_count is not None else (post.comment_count or 0)),
            is_anonymous=bool(post.is_anonymous),
            is_liked=post.id in liked_ids,
            post_code=post.post_code or "",
            property_id=uuid_str(post.property_id),
            currency=post.currency or "INR",
            is_pinned=bool(post.is_pinned),
            pinned_at=self._convert_timestamp(post.pinned_at),
            share_count=int(post.share_count or 0),
            view_count=int(post.view_count or 0),
            allow_comments=bool(getattr(post, "allow_comments", True)),
            allow_share=bool(getattr(post, "allow_share", True)),
            allow_reactions=bool(getattr(post, "allow_reactions", True)),
            report_count=int(getattr(post, "report_count", 0) or 0),
        )

    def CreatePost(self, request, context):
        user_id = parse_uuid(request.user_id)
        if not user_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("user_id is required")
            return post_pb2.PostResponse(success=False, message="user_id is required")

        with self._session() as (db, repo):
            try:
                post = repo.create_post(
                    user_id=user_id,
                    title=request.title,
                    content=request.content,
                    visibility=request.visibility,
                    post_type=request.type,
                    location=request.location,
                    latitude=getattr(request, "latitude", None) or None,
                    longitude=getattr(request, "longitude", None) or None,
                    price=request.price or None,
                    status=request.status,
                    is_anonymous=getattr(request, "is_anonymous", False),
                    property_id=parse_uuid(getattr(request, "property_id", None)),
                    currency=getattr(request, "currency", None) or "INR",
                    commit=False,
                )

                for media in request.media:
                    file_path = getattr(media, "file_path", None)
                    content_type = getattr(media, "content_type", None)
                    file_name = getattr(media, "file_name", None)
                    if not file_path:
                        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                        db.rollback()
                        return post_pb2.PostResponse(success=False, message="file_path is required for media upload")

                    inferred_type = normalize_media_type(
                        media.media_type,
                        content_type=content_type,
                    )
                    media_id = repo.add_post_media(
                        post_id=post.id,
                        uploaded_by=user_id,
                        media_type=inferred_type,
                        file_url="",
                        display_order=media.media_order or 1,
                        file_name=file_name,
                        mime_type=content_type,
                        commit=False,
                    )
                    fn = file_name or (file_path.split("/")[-1] if file_path else None)
                    key = build_post_key(post.id, media_id, fn, content_type)
                    public_url, size_bytes = upload_file_to_s3(
                        file_path=file_path,
                        key=key,
                        content_type=content_type,
                    )
                    repo.update_media_url_size(media_id, public_url, size_bytes, commit=False)

                db.commit()
                post = repo.get_post(post.id)
                return post_pb2.PostResponse(
                    success=True,
                    message="Post created successfully",
                    post=self._convert_to_proto_post(post, repository=repo),
                )
            except Exception as e:
                db.rollback()
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.PostResponse(success=False, message=f"Failed to create post: {e}")

    def GetPost(self, request, context):
        post_id = parse_uuid(request.post_id)
        if not post_id:
            return post_pb2.PostResponse(success=False, message="post_id is required")

        with self._session() as (_, repo):
            try:
                post = repo.get_post(post_id)
                if not post:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.PostResponse(success=False, message="Post not found")
                return post_pb2.PostResponse(
                    success=True,
                    message="Post retrieved successfully",
                    post=self._convert_to_proto_post(post, repository=repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostResponse(success=False, message=f"Failed to get post: {e}")

    def UpdatePost(self, request, context):
        post_id = parse_uuid(request.post_id)
        if not post_id:
            return post_pb2.PostResponse(success=False, message="post_id is required")

        kwargs = {}
        for field, value in request.ListFields():
            name = field.name
            if name == "post_id":
                continue
            if name == "type":
                kwargs["post_type"] = value
            elif name == "property_id":
                kwargs["property_id"] = parse_uuid(value)
            else:
                kwargs[name] = value

        if not kwargs:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return post_pb2.PostResponse(success=False, message="No fields to update")

        with self._session() as (_, repo):
            try:
                post = repo.update_post(post_id=post_id, **kwargs)
                if not post:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.PostResponse(success=False, message="Post not found")
                return post_pb2.PostResponse(
                    success=True,
                    message="Post updated successfully",
                    post=self._convert_to_proto_post(post, include_comments=False, repository=repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostResponse(success=False, message=f"Failed to update post: {e}")

    def DeletePost(self, request, context):
        post_id = parse_uuid(request.post_id)
        if not post_id:
            return post_pb2.GenericResponse(success=False, message="post_id is required")

        with self._session() as (_, repo):
            try:
                success = repo.delete_post(post_id)
                if not success:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.GenericResponse(success=False, message="Post not found")
                return post_pb2.GenericResponse(success=True, message="Post deleted successfully")
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.GenericResponse(success=False, message=f"Failed to delete post: {e}")

    def _list_posts_response(self, posts, total, page, limit, viewer_user_id, repo):
        liked_ids = set()
        viewer_id = parse_uuid(viewer_user_id)
        post_ids = [p.id for p in posts]
        if viewer_id and post_ids:
            try:
                liked_ids = repo.get_liked_post_ids(viewer_id, post_ids)
            except Exception:
                liked_ids = set()

        media_map = repo.get_posts_media_map(post_ids) if post_ids else {}
        like_map = repo.get_posts_like_counts(post_ids) if post_ids else {}
        comment_map = repo.get_posts_comment_counts(post_ids) if post_ids else {}
        total_pages = max(1, (total + limit - 1) // limit) if limit else 1

        return post_pb2.PostListResponse(
            success=True,
            message="Posts retrieved successfully",
            posts=[
                self._convert_to_proto_post(
                    p,
                    liked_ids,
                    include_comments=False,
                    repository=repo,
                    media_rows=media_map.get(p.id, []),
                    like_count=like_map.get(p.id, p.like_count),
                    comment_count=comment_map.get(p.id, p.comment_count),
                )
                for p in posts
            ],
            total_count=total,
            page=page,
            total_pages=total_pages,
        )

    def GetPostsByUser(self, request, context):
        user_id = parse_uuid(request.user_id)
        if not user_id:
            return post_pb2.PostListResponse(success=False, message="user_id is required")

        with self._session() as (_, repo):
            try:
                posts, total = repo.get_posts_by_user(user_id, request.page, request.limit)
                return self._list_posts_response(
                    posts, total, request.page, request.limit, request.viewer_user_id, repo
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=f"Failed to get posts: {e}")

    def SearchPosts(self, request, context):
        page = max(1, request.page)
        limit = max(1, min(100, request.limit))
        with self._session() as (_, repo):
            try:
                posts, total = repo.search_posts(
                    type=request.type,
                    location=request.location,
                    min_price=request.min_price,
                    max_price=request.max_price,
                    status=request.status,
                    query=getattr(request, "query", None),
                    hashtag=getattr(request, "hashtag", None),
                    page=page,
                    limit=limit,
                )
                return self._list_posts_response(
                    posts, total, page, limit, request.viewer_user_id, repo
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=f"Failed to search posts: {e}")

    def TrendingPosts(self, request, context):
        limit = max(1, min(50, request.limit or 10))
        with self._session() as (_, repo):
            try:
                posts = repo.get_trending_posts(limit=limit)
                return self._list_posts_response(posts, len(posts), 1, limit, request.viewer_user_id, repo)
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=f"Failed to get trending posts: {e}")

    def AddPostMedia(self, request, context):
        post_id = parse_uuid(request.post_id)
        uploaded_by = parse_uuid(getattr(request, "uploaded_by", None))
        if not post_id:
            return post_pb2.PostResponse(success=False, message="post_id is required")

        with self._session() as (_, repo):
            try:
                post = repo.get_post(post_id)
                if not post:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.PostResponse(success=False, message="Post not found")

                uploader = uploaded_by or post.user_id
                for media in request.media:
                    file_path = getattr(media, "file_path", None)
                    content_type = getattr(media, "content_type", None)
                    if not file_path:
                        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                        return post_pb2.PostResponse(success=False, message="file_path is required for media upload")

                    media_id = repo.add_post_media(
                        post_id=post.id,
                        uploaded_by=uploader,
                        media_type=normalize_media_type(
                            media.media_type,
                            content_type=content_type,
                        ),
                        file_url="",
                        display_order=media.media_order or 1,
                        file_name=getattr(media, "file_name", None),
                        mime_type=content_type,
                    )
                    file_name = getattr(media, "file_name", None) or file_path.split("/")[-1]
                    key = build_post_key(post.id, media_id, file_name, content_type)
                    public_url, size_bytes = upload_file_to_s3(
                        file_path=file_path,
                        key=key,
                        content_type=content_type,
                    )
                    repo.update_media_url_size(media_id, public_url, size_bytes)

                post = repo.get_post(post_id)
                return post_pb2.PostResponse(
                    success=True,
                    message="Media added successfully",
                    post=self._convert_to_proto_post(post, repository=repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostResponse(success=False, message=f"Failed to add media: {e}")

    def DeletePostMedia(self, request, context):
        media_id = parse_uuid(request.media_id)
        if not media_id:
            return post_pb2.GenericResponse(success=False, message="media_id is required")

        with self._session() as (_, repo):
            try:
                success = repo.delete_post_media(media_id)
                if not success:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.GenericResponse(success=False, message="Media not found")
                return post_pb2.GenericResponse(success=True, message="Media deleted successfully")
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.GenericResponse(success=False, message=f"Failed to delete media: {e}")

    def LikePost(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.PostResponse(success=False, message="post_id and user_id are required")

        with self._session() as (_, repo):
            try:
                post = repo.like_post(post_id, user_id, request.reaction_type)
                return post_pb2.PostResponse(
                    success=True,
                    message="Post liked successfully",
                    post=self._convert_to_proto_post(post, repository=repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostResponse(success=False, message=f"Failed to like post: {e}")

    def UnlikePost(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.PostResponse(success=False, message="post_id and user_id are required")

        with self._session() as (_, repo):
            try:
                post = repo.unlike_post(post_id, user_id)
                return post_pb2.PostResponse(
                    success=True,
                    message="Post unliked successfully",
                    post=self._convert_to_proto_post(post, repository=repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostResponse(success=False, message=f"Failed to unlike post: {e}")

    def CreateComment(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.CommentResponse(success=False, message="post_id and user_id are required")

        with self._session() as (_, repo):
            try:
                comment = repo.create_comment(
                    post_id=post_id,
                    user_id=user_id,
                    content=request.comment,
                    parent_comment_id=parse_uuid(request.parent_comment_id),
                    is_anonymous=getattr(request, "is_anonymous", False),
                )
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment created successfully",
                    comment=self._convert_to_proto_comment(comment, repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.CommentResponse(success=False, message=str(e))

    def UpdateComment(self, request, context):
        comment_id = parse_uuid(request.comment_id)
        if not comment_id:
            return post_pb2.CommentResponse(success=False, message="comment_id is required")

        with self._session() as (_, repo):
            try:
                comment = repo.update_comment(
                    comment_id=comment_id,
                    content=request.comment or None,
                    status=request.status or None,
                )
                if not comment:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.CommentResponse(success=False, message="Comment not found")
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment updated successfully",
                    comment=self._convert_to_proto_comment(comment, repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.CommentResponse(success=False, message=str(e))

    def DeleteComment(self, request, context):
        comment_id = parse_uuid(request.comment_id)
        if not comment_id:
            return post_pb2.GenericResponse(success=False, message="comment_id is required")

        with self._session() as (_, repo):
            try:
                success = repo.delete_comment(comment_id)
                if not success:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return post_pb2.GenericResponse(success=False, message="Comment not found")
                return post_pb2.GenericResponse(success=True, message="Comment deleted successfully")
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.GenericResponse(success=False, message=f"Failed to delete comment: {e}")

    def GetComments(self, request, context):
        post_id = parse_uuid(request.post_id)
        if not post_id:
            return post_pb2.CommentListResponse(success=False, message="post_id is required")

        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 10))

        with self._session() as (_, repo):
            try:
                total_comments = repo.get_post_comment_count(post_id)
                total_pages = max(1, (total_comments + limit - 1) // limit)
                page = min(page, total_pages)
                comments, total = repo.get_comments(post_id=post_id, page=page, limit=limit)
                return post_pb2.CommentListResponse(
                    success=True,
                    message="Comments retrieved successfully",
                    comments=[self._convert_to_proto_comment(c, repo) for c in comments],
                    total_count=total,
                    page=page,
                    total_pages=total_pages,
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.CommentListResponse(success=False, message=f"Failed to get comments: {e}")

    def LikeComment(self, request, context):
        comment_id = parse_uuid(request.comment_id)
        user_id = parse_uuid(request.user_id)
        if not comment_id or not user_id:
            return post_pb2.CommentResponse(success=False, message="comment_id and user_id are required")

        with self._session() as (_, repo):
            try:
                comment = repo.like_comment(comment_id, user_id, request.reaction_type)
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment liked successfully",
                    comment=self._convert_to_proto_comment(comment, repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.CommentResponse(success=False, message=f"Failed to like comment: {e}")

    def UnlikeComment(self, request, context):
        comment_id = parse_uuid(request.comment_id)
        user_id = parse_uuid(request.user_id)
        if not comment_id or not user_id:
            return post_pb2.CommentResponse(success=False, message="comment_id and user_id are required")

        with self._session() as (_, repo):
            try:
                comment = repo.unlike_comment(comment_id, user_id)
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment unliked successfully",
                    comment=self._convert_to_proto_comment(comment, repo),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.CommentResponse(success=False, message=f"Failed to unlike comment: {e}")

    # ------------------------------------------------------------------ posts
    def GetMyPosts(self, request, context):
        user_id = parse_uuid(request.user_id)
        if not user_id:
            return post_pb2.PostListResponse(success=False, message="user_id is required")
        with self._session() as (_, repo):
            try:
                posts, total = repo.get_posts_by_user(user_id, request.page, request.limit)
                return self._list_posts_response(posts, total, request.page, request.limit, user_id, repo)
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=str(e))

    def GetPublicPosts(self, request, context):
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            try:
                posts, total = repo.get_public_posts(page=page, limit=limit)
                return self._list_posts_response(posts, total, page, limit, request.viewer_user_id, repo)
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=str(e))

    def GetPropertyPosts(self, request, context):
        property_id = parse_uuid(request.property_id)
        if not property_id:
            return post_pb2.PostListResponse(success=False, message="property_id is required")
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            try:
                posts, total = repo.get_property_posts(property_id, page, limit)
                return self._list_posts_response(posts, total, page, limit, request.viewer_user_id, repo)
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=str(e))

    def GetBuilderPosts(self, request, context):
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        builder_id = parse_uuid(getattr(request, "builder_user_id", None))
        user_ids = [uid for uid in (parse_uuid(x) for x in request.user_ids) if uid]
        with self._session() as (_, repo):
            try:
                posts, total = repo.get_builder_posts(
                    builder_user_id=builder_id,
                    user_ids=user_ids or None,
                    page=page,
                    limit=limit,
                )
                return self._list_posts_response(posts, total, page, limit, request.viewer_user_id, repo)
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostListResponse(success=False, message=str(e))

    def PinPost(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.PostResponse(success=False, message="post_id and user_id are required")
        with self._session() as (_, repo):
            post = repo.pin_post(post_id, user_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.PostResponse(success=False, message="Post not found or not authorized")
            return post_pb2.PostResponse(success=True, message="Post pinned", post=self._convert_to_proto_post(post, repository=repo))

    def UnpinPost(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.PostResponse(success=False, message="post_id and user_id are required")
        with self._session() as (_, repo):
            post = repo.unpin_post(post_id, user_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.PostResponse(success=False, message="Post not found or not authorized")
            return post_pb2.PostResponse(success=True, message="Post unpinned", post=self._convert_to_proto_post(post, repository=repo))

    def ArchivePost(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.PostResponse(success=False, message="post_id and user_id are required")
        with self._session() as (_, repo):
            post = repo.archive_post(post_id, user_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.PostResponse(success=False, message="Post not found or not authorized")
            return post_pb2.PostResponse(success=True, message="Post archived", post=self._convert_to_proto_post(post, repository=repo))

    def RestoreArchivedPost(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.PostResponse(success=False, message="post_id and user_id are required")
        with self._session() as (_, repo):
            post = repo.restore_archived_post(post_id, user_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.PostResponse(success=False, message="Post not found or not authorized")
            return post_pb2.PostResponse(success=True, message="Post restored", post=self._convert_to_proto_post(post, repository=repo))

    # ------------------------------------------------------------------ likes
    def GetPostLikes(self, request, context):
        post_id = parse_uuid(request.post_id)
        if not post_id:
            return post_pb2.PostLikeListResponse(success=False, message="post_id is required")
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            try:
                likes, total = repo.get_post_likes(post_id, page, limit)
                total_pages = max(1, (total + limit - 1) // limit)
                return post_pb2.PostLikeListResponse(
                    success=True,
                    message="Post likes retrieved",
                    likes=[
                        post_pb2.PostLikeUser(
                            user_id=uuid_str(like.user_id),
                            first_name="",
                            last_name="",
                            user_role="",
                            reaction_type=like.reaction_type or "LIKE",
                            liked_at=self._convert_timestamp(like.created_at),
                        )
                        for like in likes
                    ],
                    total_count=total,
                    page=page,
                    total_pages=total_pages,
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.PostLikeListResponse(success=False, message=str(e))

    def CheckLikeStatus(self, request, context):
        post_id = parse_uuid(request.post_id)
        user_id = parse_uuid(request.user_id)
        if not post_id or not user_id:
            return post_pb2.CheckLikeStatusResponse(success=False, message="post_id and user_id are required")
        with self._session() as (_, repo):
            is_liked, reaction = repo.check_like_status(post_id, user_id)
            return post_pb2.CheckLikeStatusResponse(
                success=True,
                message="Like status retrieved",
                is_liked=is_liked,
                reaction_type=reaction or "",
            )

    # ---------------------------------------------------------------- comments
    def GetComment(self, request, context):
        comment_id = parse_uuid(request.comment_id)
        if not comment_id:
            return post_pb2.CommentResponse(success=False, message="comment_id is required")
        with self._session() as (_, repo):
            comment = repo.get_comment_with_user(comment_id)
            if not comment:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.CommentResponse(success=False, message="Comment not found")
            return post_pb2.CommentResponse(
                success=True,
                message="Comment retrieved",
                comment=self._convert_to_proto_comment(comment, repo),
            )

    def ReplyComment(self, request, context):
        if not parse_uuid(request.parent_comment_id):
            return post_pb2.CommentResponse(success=False, message="parent_comment_id is required for replies")
        return self.CreateComment(request, context)

    def GetReplies(self, request, context):
        comment_id = parse_uuid(request.comment_id)
        if not comment_id:
            return post_pb2.CommentListResponse(success=False, message="comment_id is required")
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            replies, total = repo.get_replies(comment_id, page, limit)
            total_pages = max(1, (total + limit - 1) // limit)
            return post_pb2.CommentListResponse(
                success=True,
                message="Replies retrieved",
                comments=[self._convert_to_proto_comment(c, repo) for c in replies],
                total_count=total,
                page=page,
                total_pages=total_pages,
            )

    def ReportComment(self, request, context):
        return self._create_report(
            context,
            entity_type="COMMENT",
            entity_id=parse_uuid(request.comment_id),
            reported_by=parse_uuid(request.reported_by),
            reported_user_id=parse_uuid(request.reported_user_id),
            reason_code=request.reason_code,
            description=request.description,
        )

    # ------------------------------------------------------------------ share
    def _convert_to_proto_share(self, share, post=None, repository=None):
        return post_pb2.PostShare(
            id=uuid_str(share.id),
            share_code=share.share_code or "",
            post_id=uuid_str(share.post_id),
            shared_by=uuid_str(share.shared_by),
            share_type=share.share_type or "SHARE",
            caption=share.caption or "",
            visibility=share.visibility or "PUBLIC",
            created_at=self._convert_timestamp(share.created_at),
            post=self._convert_to_proto_post(post, repository=repository) if post else None,
        )

    def SharePost(self, request, context):
        post_id = parse_uuid(request.post_id)
        shared_by = parse_uuid(request.shared_by)
        if not post_id or not shared_by:
            return post_pb2.PostShareResponse(success=False, message="post_id and shared_by are required")
        with self._session() as (_, repo):
            share = repo.share_post(
                post_id=post_id,
                shared_by=shared_by,
                share_type=request.share_type,
                caption=request.caption,
                visibility=request.visibility,
            )
            if not share:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.PostShareResponse(success=False, message="Post not found or sharing disabled")
            post = repo.get_post(post_id)
            return post_pb2.PostShareResponse(
                success=True,
                message="Post shared",
                share=self._convert_to_proto_share(share, post, repo),
            )

    def GetSharedPosts(self, request, context):
        user_id = parse_uuid(request.user_id)
        if not user_id:
            return post_pb2.PostShareListResponse(success=False, message="user_id is required")
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            shares, total = repo.get_shared_posts(user_id, page, limit)
            total_pages = max(1, (total + limit - 1) // limit)
            items = []
            for share in shares:
                post = repo.get_post(share.post_id)
                items.append(self._convert_to_proto_share(share, post, repo))
            return post_pb2.PostShareListResponse(
                success=True,
                message="Shared posts retrieved",
                shares=items,
                total_count=total,
                page=page,
                total_pages=total_pages,
            )

    def DeleteSharedPost(self, request, context):
        share_id = parse_uuid(request.share_id)
        user_id = parse_uuid(request.user_id)
        if not share_id or not user_id:
            return post_pb2.GenericResponse(success=False, message="share_id and user_id are required")
        with self._session() as (_, repo):
            if not repo.delete_shared_post(share_id, user_id):
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return post_pb2.GenericResponse(success=False, message="Share not found or not authorized")
            return post_pb2.GenericResponse(success=True, message="Shared post deleted")

    # ----------------------------------------------------------------- reports
    def _convert_to_proto_report(self, report):
        return post_pb2.Report(
            id=uuid_str(report.id),
            report_code=report.report_code or "",
            entity_type=report.entity_type or "",
            entity_id=uuid_str(report.entity_id),
            reported_by=uuid_str(report.reported_by),
            reported_user_id=uuid_str(report.reported_user_id),
            reason_code=report.reason_code or "",
            description=report.description or "",
            status=report.status or "PENDING",
            priority=report.priority or "MEDIUM",
            created_at=self._convert_timestamp(report.created_at),
            reviewed_by=uuid_str(report.reviewed_by),
            reviewed_at=self._convert_timestamp(report.reviewed_at),
            action_taken=report.action_taken or "",
            action_note=report.action_note or "",
        )

    def _create_report(self, context, entity_type, entity_id, reported_by, reported_user_id, reason_code, description):
        if not entity_id or not reported_by:
            return post_pb2.ReportResponse(success=False, message="entity_id and reported_by are required")
        with self._session() as (_, repo):
            try:
                report = repo.create_report(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    reported_by=reported_by,
                    reported_user_id=reported_user_id,
                    reason_code=reason_code,
                    description=description,
                )
                return post_pb2.ReportResponse(
                    success=True,
                    message="Report submitted",
                    report=self._convert_to_proto_report(report),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.ReportResponse(success=False, message=str(e))

    def ReportPost(self, request, context):
        return self._create_report(
            context,
            entity_type="POST",
            entity_id=parse_uuid(request.post_id),
            reported_by=parse_uuid(request.reported_by),
            reported_user_id=parse_uuid(request.reported_user_id),
            reason_code=request.reason_code,
            description=request.description,
        )

    def ReportProperty(self, request, context):
        return self._create_report(
            context,
            entity_type="PROPERTY",
            entity_id=parse_uuid(request.property_id),
            reported_by=parse_uuid(request.reported_by),
            reported_user_id=parse_uuid(request.reported_user_id),
            reason_code=request.reason_code,
            description=request.description,
        )

    def ReportUser(self, request, context):
        reported_user_id = parse_uuid(request.user_id)
        return self._create_report(
            context,
            entity_type="USER",
            entity_id=reported_user_id,
            reported_by=parse_uuid(request.reported_by),
            reported_user_id=reported_user_id,
            reason_code=request.reason_code,
            description=request.description,
        )

    def GetMyReports(self, request, context):
        reported_by = parse_uuid(request.reported_by)
        if not reported_by:
            return post_pb2.ReportListResponse(success=False, message="reported_by is required")
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            reports, total = repo.get_reports_by_user(reported_by, page, limit)
            total_pages = max(1, (total + limit - 1) // limit)
            return post_pb2.ReportListResponse(
                success=True,
                message="Reports retrieved",
                reports=[self._convert_to_proto_report(r) for r in reports],
                total_count=total,
                page=page,
                total_pages=total_pages,
            )

    def GetReport(self, request, context):
        report_id = parse_uuid(request.report_id)
        if not report_id:
            return post_pb2.ReportResponse(success=False, message="report_id is required")
        with self._session() as (_, repo):
            report = repo.get_report(report_id)
            if not report:
                return post_pb2.ReportResponse(success=False, message="Report not found")
            return post_pb2.ReportResponse(
                success=True,
                message="Report retrieved",
                report=self._convert_to_proto_report(report),
            )

    def GetReports(self, request, context):
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            reports, total = repo.get_reports(
                status=request.status or None,
                entity_type=request.entity_type or None,
                priority=request.priority or None,
                reported_by=parse_uuid(request.reported_by),
                reported_user_id=parse_uuid(request.reported_user_id),
                entity_id=parse_uuid(request.entity_id),
                page=page,
                limit=limit,
            )
            total_pages = max(1, (total + limit - 1) // limit)
            return post_pb2.ReportListResponse(
                success=True,
                message="Reports retrieved",
                reports=[self._convert_to_proto_report(r) for r in reports],
                total_count=total,
                page=page,
                total_pages=total_pages,
            )

    def UpdateReportStatus(self, request, context):
        report_id = parse_uuid(request.report_id)
        if not report_id or not (request.status or "").strip():
            return post_pb2.ReportResponse(success=False, message="report_id and status are required")
        with self._session() as (_, repo):
            try:
                report = repo.update_report_status(
                    report_id=report_id,
                    status=request.status,
                    reviewed_by=parse_uuid(request.reviewed_by),
                    action_taken=request.action_taken or None,
                    action_note=request.action_note or None,
                    priority=request.priority or None,
                )
                if not report:
                    return post_pb2.ReportResponse(success=False, message="Report not found")
                return post_pb2.ReportResponse(
                    success=True,
                    message="Report updated",
                    report=self._convert_to_proto_report(report),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                return post_pb2.ReportResponse(success=False, message=str(e))

    def AssignReport(self, request, context):
        report_id = parse_uuid(request.report_id)
        reviewer = parse_uuid(request.reviewed_by)
        if not report_id or not reviewer:
            return post_pb2.ReportResponse(success=False, message="report_id and reviewed_by are required")
        with self._session() as (_, repo):
            report = repo.assign_report(report_id, reviewer)
            if not report:
                return post_pb2.ReportResponse(success=False, message="Report not found")
            return post_pb2.ReportResponse(
                success=True,
                message="Report assigned",
                report=self._convert_to_proto_report(report),
            )

    def GetReportsByEntity(self, request, context):
        entity_id = parse_uuid(request.entity_id)
        entity_type = (request.entity_type or "").strip().upper()
        if not entity_id or not entity_type:
            return post_pb2.ReportListResponse(success=False, message="entity_type and entity_id are required")
        page = max(1, request.page or 1)
        limit = max(1, min(100, request.limit or 20))
        with self._session() as (_, repo):
            reports, total = repo.get_reports(
                entity_type=entity_type,
                entity_id=entity_id,
                page=page,
                limit=limit,
            )
            total_pages = max(1, (total + limit - 1) // limit)
            return post_pb2.ReportListResponse(
                success=True,
                message="Reports retrieved",
                reports=[self._convert_to_proto_report(r) for r in reports],
                total_count=total,
                page=page,
                total_pages=total_pages,
            )

    def GetReportStats(self, request, context):
        with self._session() as (_, repo):
            stats = repo.get_report_stats()
            return post_pb2.ReportStatsResponse(success=True, message="OK", **stats)

    def DeleteReport(self, request, context):
        report_id = parse_uuid(request.report_id)
        if not report_id:
            return post_pb2.GenericResponse(success=False, message="report_id is required")
        with self._session() as (_, repo):
            ok = repo.delete_report(report_id)
            if not ok:
                return post_pb2.GenericResponse(success=False, message="Report not found")
            return post_pb2.GenericResponse(success=True, message="Report deleted")


def serve():
    port = os.getenv("PORT", "50055")
    get_db_engine()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[AuthServerInterceptor()],
    )
    post_pb2_grpc.add_PostsServiceServicer_to_server(PostsService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"Posts service started on port {port} (schemas: post, user, api_gateway)")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

import grpc
import os
from concurrent import futures
from dotenv import load_dotenv
from ..proto_files import post_pb2, post_pb2_grpc
from ..repository.post_repository import PostRepository
from ..utils.db_connection import get_db_engine
from sqlalchemy.orm import sessionmaker
from ..entity.user_entity import User
from ..interceptors.auth_interceptor import AuthServerInterceptor
from ..utils.s3_utils import upload_base64_to_s3, upload_file_to_s3, build_post_key

from uuid import uuid4
# Load environment variables
load_dotenv()

# Create database session factory
SessionLocal = sessionmaker(bind=get_db_engine(), expire_on_commit=False)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class PostsService(post_pb2_grpc.PostsServiceServicer):
    def __init__(self):
        self._SessionLocal = SessionLocal
        self._open_session()

    def _open_session(self):
        self.db = self._SessionLocal()
        self.repository = PostRepository(self.db)

    def _reset_session(self):
        """Recover from Neon SSL drops / poisoned transactions."""
        try:
            self.db.rollback()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass
        self._open_session()

    def _with_db(self, fn):
        try:
            return fn()
        except Exception:
            self._reset_session()
            raise

    def _convert_timestamp(self, dt):
        return int(dt.timestamp()) if dt else 0

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

        repo = repository or self.repository
        # Prefer pre-batched media/counts (Search/Trending) to avoid N+1 queries.
        if media_rows is None:
            if include_media:
                try:
                    media_rows = repo.get_post_media(post.id)
                except Exception:
                    media_rows = []
            else:
                media_rows = []

        liked_ids = liked_post_ids or set()
        comments = []
        if include_comments:
            try:
                comments = [self._convert_to_proto_comment(c) for c in (post.comments or [])]
            except Exception:
                comments = []

        if comment_count is None:
            try:
                comment_count = repo.get_post_comment_count(post.id)
            except Exception:
                try:
                    comment_count = len(post.comments) if getattr(post, 'comments', None) is not None else 0
                except Exception:
                    comment_count = 0

        if like_count is None:
            try:
                like_count = repo.get_post_like_count(post.id)
            except Exception:
                try:
                    like_count = len(post.likes) if getattr(post, 'likes', None) is not None else 0
                except Exception:
                    like_count = 0

        return post_pb2.Post(
            id=post.id,
            user_id=post.user_id,
            user_first_name=post.user.first_name if post.user else "",
            user_last_name=post.user.last_name if post.user else "",
            user_email=post.user.email if post.user else "",
            user_phone=post.user.phone if post.user else "",
            user_role=post.user.role if post.user else "",
            title=post.title or "",
            content=post.content or "",
            visibility=post.visibility or "",
            type=post.type or "",
            location=post.location or "",
            # Include new lat/lng
            latitude=float(post.latitude) if getattr(post, 'latitude', None) is not None else 0.0,
            longitude=float(post.longitude) if getattr(post, 'longitude', None) is not None else 0.0,
            price=float(post.price) if post.price else 0.0,
            status=post.status or "",
            created_at=self._convert_timestamp(post.created_at),
            media=[self._convert_to_proto_media(m, post_id=post.id) for m in (media_rows or [])],
            comments=comments,
            like_count=int(like_count or 0),
            comment_count=int(comment_count or 0),
            is_liked=post.id in liked_ids,
        )

    def _convert_to_proto_media(self, media, post_id: int = 0):
        # Support SQLAlchemy ORM objects and Core Row objects
        def _get(field):
            if hasattr(media, field):
                return getattr(media, field)
            mapping = getattr(media, "_mapping", None)
            if mapping and field in mapping:
                return mapping[field]
            return None

        return post_pb2.PostMedia(
            id=_get('id') or 0,
            post_id=post_id or _get('post_id') or 0,
            media_type=_get('media_type') or "",
            media_url=_get('media_url') or "",
            media_order=_get('media_order') or 0,
            media_size=_get('media_size') or 0,
            caption=_get('caption') or "",
            uploaded_at=self._convert_timestamp(_get('uploaded_at'))
        )

    def _convert_to_proto_comment(self, comment):
        return post_pb2.Comment(
            id=comment.id,
            post_id=comment.post_id,
            parent_comment_id=comment.parent_comment_id or 0,
            comment=comment.comment,
            user_id=comment.user_id,
            user_first_name=comment.user.first_name if comment.user else "",
            user_last_name=comment.user.last_name if comment.user else "",
            user_role=comment.user.role if comment.user else "",
            status=comment.status,
            added_at=self._convert_timestamp(comment.added_at),
            commented_at=self._convert_timestamp(comment.commented_at),
            replies=[self._convert_to_proto_comment(r) for r in comment.replies],
            like_count=len(comment.likes)
        )

    def CreatePost(self, request, context):
        try:
            # First check if user exists
            user = self.db.query(User).filter(User.id == request.user_id).first()
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User with id {request.user_id} not found")
                return post_pb2.PostResponse(
                    success=False,
                    message=f"User with id {request.user_id} not found"
                )

            try:
                # Begin explicit transaction
                post = self.repository.create_post(
                    user_id=request.user_id,
                    title=request.title,
                    content=request.content,
                    visibility=request.visibility,
                    type=request.type,
                    location=request.location,
                    latitude=getattr(request, 'latitude', None),
                    longitude=getattr(request, 'longitude', None),
                    price=request.price,
                    status=request.status,
                    is_anonymous=getattr(request, 'is_anonymous', False),
                    commit=False,
                )

                created_media_ids = []
                # Handle media uploads if any
                for media in request.media:
                    # Require file_path for uploads (supports images and videos)
                    file_path = getattr(media, 'file_path', None)
                    content_type = getattr(media, 'content_type', None)
                    file_name = getattr(media, 'file_name', None)
                    if not file_path:
                        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                        context.set_details("file_path is required for media upload")
                        self.db.rollback()
                        return post_pb2.PostResponse(success=False, message="file_path is required for media upload")

                    # 1) Create media row first to obtain media_id (no commit yet)
                    temp_url = ""
                    inferred_type = media.media_type or (('video' if (content_type or '').startswith('video/') else 'image'))
                    media_id = self.repository.add_post_media(
                        post_id=post.id,
                        media_type=inferred_type,
                        media_url=temp_url,
                        media_order=media.media_order,
                        media_size=0,
                        caption=media.caption,
                        commit=False,
                    )
                    created_media_ids.append(media_id)

                    # 2) Build S3 key and upload
                    fn = file_name or (file_path.split('/')[-1] if file_path else None)
                    key = build_post_key(post.id, media_id, fn, content_type)
                    public_url, size_bytes = upload_file_to_s3(
                        file_path=file_path,
                        key=key,
                        content_type=content_type,
                    )

                    # 3) Update media row (no commit yet)
                    self.repository.update_media_url_size(media_id, public_url, size_bytes, commit=False)

                # All operations succeeded; commit once
                self.db.commit()

                # Refresh post and return
                post = self.repository.get_post(post.id)
                return post_pb2.PostResponse(
                    success=True,
                    message="Post created successfully",
                    post=self._convert_to_proto_post(post)
                )
            except Exception as e:
                # Rollback if any step failed
                self.db.rollback()
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.PostResponse(
                    success=False,
                    message=f"Failed to create post: {str(e)}"
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostResponse(
                success=False,
                message=f"Failed to create post: {str(e)}"
            )

    def GetPost(self, request, context):
        try:
            post = self.repository.get_post(request.post_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Post not found")
                return post_pb2.PostResponse(
                    success=False,
                    message="Post not found"
                )

            return post_pb2.PostResponse(
                success=True,
                message="Post retrieved successfully",
                post=self._convert_to_proto_post(post)
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostResponse(
                success=False,
                message=f"Failed to get post: {str(e)}"
            )

    def UpdatePost(self, request, context):
        try:
            # Proto3 scalar defaults ("" / 0 / False) are indistinguishable from
            # "unset" unless we only apply fields that were explicitly set.
            kwargs = {}
            for field, value in request.ListFields():
                name = field.name
                if name == "post_id":
                    continue
                kwargs[name] = value

            if not kwargs:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("No fields to update")
                return post_pb2.PostResponse(
                    success=False,
                    message="No fields to update",
                )

            post = self.repository.update_post(post_id=request.post_id, **kwargs)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Post not found")
                return post_pb2.PostResponse(
                    success=False,
                    message="Post not found"
                )

            return post_pb2.PostResponse(
                success=True,
                message="Post updated successfully",
                post=self._convert_to_proto_post(post, include_comments=False)
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostResponse(
                success=False,
                message=f"Failed to update post: {str(e)}"
            )

    def DeletePost(self, request, context):
        try:
            success = self.repository.delete_post(request.post_id)
            if not success:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Post not found")
                return post_pb2.GenericResponse(
                    success=False,
                    message="Post not found"
                )

            return post_pb2.GenericResponse(
                success=True,
                message="Post deleted successfully"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.GenericResponse(
                success=False,
                message=f"Failed to delete post: {str(e)}"
            )

    def GetPostsByUser(self, request, context):
        try:
            posts, total = self.repository.get_posts_by_user(
                user_id=request.user_id,
                page=request.page,
                limit=request.limit
            )

            viewer_id = getattr(request, "viewer_user_id", 0) or 0
            liked_ids = set()
            if viewer_id:
                try:
                    liked_ids = self.repository.get_liked_post_ids(
                        viewer_id, [p.id for p in posts]
                    )
                except Exception:
                    liked_ids = set()

            post_ids = [p.id for p in posts]
            media_map = {}
            like_map = {}
            comment_map = {}
            try:
                media_map = self.repository.get_posts_media_map(post_ids)
                like_map = self.repository.get_posts_like_counts(post_ids)
                comment_map = self.repository.get_posts_comment_counts(post_ids)
            except Exception:
                pass

            return post_pb2.PostListResponse(
                success=True,
                message="Posts retrieved successfully",
                posts=[
                    self._convert_to_proto_post(
                        p,
                        liked_ids,
                        include_comments=False,
                        media_rows=media_map.get(p.id, []),
                        like_count=like_map.get(p.id, 0),
                        comment_count=comment_map.get(p.id, 0),
                    )
                    for p in posts
                ],
                total_count=total,
                page=request.page,
                total_pages=(total + request.limit - 1) // request.limit
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostListResponse(
                success=False,
                message=f"Failed to get posts: {str(e)}"
            )

    def SearchPosts(self, request, context):
        # Per-request session: Home fires search + trending in parallel; a shared
        # SQLAlchemy session is not thread-safe and causes intermittent INTERNAL errors.
        db = self._SessionLocal()
        repo = PostRepository(db)
        try:
            page = max(1, request.page)
            limit = max(1, min(100, request.limit))

            def _do():
                return repo.search_posts(
                    type=request.type,
                    location=request.location,
                    min_price=request.min_price,
                    max_price=request.max_price,
                    status=request.status,
                    page=page,
                    limit=limit
                )

            try:
                posts, total = _do()
            except Exception:
                db.rollback()
                posts, total = _do()

            viewer_id = getattr(request, "viewer_user_id", 0) or 0
            liked_ids = set()
            if viewer_id:
                try:
                    liked_ids = repo.get_liked_post_ids(viewer_id, [p.id for p in posts])
                except Exception:
                    liked_ids = set()

            total_pages = (total + limit - 1) // limit
            if total_pages == 0:
                total_pages = 1

            post_ids = [p.id for p in posts]
            media_map = {}
            like_map = {}
            comment_map = {}
            try:
                media_map = repo.get_posts_media_map(post_ids)
                like_map = repo.get_posts_like_counts(post_ids)
                comment_map = repo.get_posts_comment_counts(post_ids)
            except Exception:
                pass

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
                        like_count=like_map.get(p.id, 0),
                        comment_count=comment_map.get(p.id, 0),
                    )
                    for p in posts
                ],
                total_count=total,
                page=page,
                total_pages=total_pages
            )
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostListResponse(
                success=False,
                message=f"Failed to search posts: {str(e)}"
            )
        finally:
            db.close()

    def TrendingPosts(self, request, context):
        db = self._SessionLocal()
        repo = PostRepository(db)
        try:
            limit = max(1, min(50, request.limit or 10))

            def _do():
                return repo.get_trending_posts(limit=limit)

            try:
                posts = _do()
            except Exception:
                db.rollback()
                posts = _do()

            viewer_id = getattr(request, "viewer_user_id", 0) or 0
            liked_ids = set()
            if viewer_id:
                try:
                    liked_ids = repo.get_liked_post_ids(viewer_id, [p.id for p in posts])
                except Exception:
                    liked_ids = set()

            post_ids = [p.id for p in posts]
            like_map = {}
            comment_map = {}
            try:
                like_map = repo.get_posts_like_counts(post_ids)
                comment_map = repo.get_posts_comment_counts(post_ids)
            except Exception:
                pass

            return post_pb2.PostListResponse(
                success=True,
                message="Trending posts retrieved successfully",
                posts=[
                    self._convert_to_proto_post(
                        p,
                        liked_ids,
                        include_comments=False,
                        repository=repo,
                        include_media=False,
                        media_rows=[],
                        like_count=like_map.get(p.id, 0),
                        comment_count=comment_map.get(p.id, 0),
                    )
                    for p in posts
                ],
                total_count=len(posts),
                page=1,
                total_pages=1,
            )
        except Exception as e:
            try:
                db.rollback()
            except Exception:
                pass
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostListResponse(
                success=False,
                message=f"Failed to get trending posts: {str(e)}"
            )
        finally:
            db.close()

    def AddPostMedia(self, request, context):
        try:
            post = self.repository.get_post(request.post_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Post not found")
                return post_pb2.PostResponse(
                    success=False,
                    message="Post not found"
                )

            for media in request.media:
                try:
                    # 1) Create media row first to obtain media_id
                    temp_url = ""
                    media_id = self.repository.add_post_media(
                        post_id=post.id,
                        media_type=media.media_type or 'image',
                        media_url=temp_url,
                        media_order=media.media_order,
                        media_size=0,
                        caption=media.caption,
                    )

                    # 2) Build S3 key using media_id and upload
                    file_path = getattr(media, 'file_path', None)
                    content_type = getattr(media, 'content_type', None)
                    if not file_path:
                        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                        context.set_details("file_path is required for media upload")
                        return post_pb2.PostResponse(success=False, message="file_path is required for media upload")

                    file_name = getattr(media, 'file_name', None) or (file_path.split('/')[-1] if file_path else 'image')
                    key = build_post_key(post.id, media_id, file_name, content_type)
                    public_url, size_bytes = upload_file_to_s3(
                        file_path=file_path,
                        key=key,
                        content_type=content_type,
                    )

                    # 3) Update media row
                    self.repository.update_media_url_size(media_id, public_url, size_bytes)
                except Exception as media_error:
                    print(f"Error adding media: {str(media_error)}")
                    continue

            # Refresh post to get updated media
            post = self.repository.get_post(request.post_id)
            return post_pb2.PostResponse(
                success=True,
                message="Media added successfully",
                post=self._convert_to_proto_post(post)
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostResponse(
                success=False,
                message=f"Failed to add media: {str(e)}"
            )

    def DeletePostMedia(self, request, context):
        try:
            success = self.repository.delete_post_media(request.media_id)
            if not success:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Media not found")
                return post_pb2.GenericResponse(
                    success=False,
                    message="Media not found"
                )

            return post_pb2.GenericResponse(
                success=True,
                message="Media deleted successfully"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.GenericResponse(
                success=False,
                message=f"Failed to delete media: {str(e)}"
            )

    def LikePost(self, request, context):
        try:
            # First check if user exists
            user = self.db.query(User).filter(User.id == request.user_id).first()
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User with id {request.user_id} not found")
                return post_pb2.PostResponse(
                    success=False,
                    message=f"User with id {request.user_id} not found"
                )

            # Check if post exists and get it first
            post = self.repository.get_post(request.post_id)  # Changed from request.id
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Post with id {request.post_id} not found")  # Changed from request.id
                return post_pb2.PostResponse(
                    success=False,
                    message=f"Post with id {request.post_id} not found"  # Changed from request.id
                )

            # Try to like the post
            try:
                # Use the post ID from the post we found
                post = self.repository.like_post(
                    post_id=post.id,
                    user_id=request.user_id,
                    reaction_type=request.reaction_type
                )
                return post_pb2.PostResponse(
                    success=True,
                    message="Post liked successfully",
                    post=self._convert_to_proto_post(post)
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.PostResponse(
                    success=False,
                    message=f"Failed to like post: {str(e)}"
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostResponse(
                success=False,
                message=f"Failed to like post: {str(e)}"
            )

    def UnlikePost(self, request, context):
        try:
            # First check if user exists
            user = self.db.query(User).filter(User.id == request.user_id).first()
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User with id {request.user_id} not found")
                return post_pb2.PostResponse(
                    success=False,
                    message=f"User with id {request.user_id} not found"
                )

            # Check if post exists and get it first
            post = self.repository.get_post(request.post_id)  # Changed from request.id
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Post with id {request.post_id} not found")  # Changed from request.id
                return post_pb2.PostResponse(
                    success=False,
                    message=f"Post with id {request.post_id} not found"  # Changed from request.id
                )

            # Try to unlike the post
            try:
                post = self.repository.unlike_post(
                    post_id=post.id,
                    user_id=request.user_id
                )
                return post_pb2.PostResponse(
                    success=True,
                    message="Post unliked successfully",
                    post=self._convert_to_proto_post(post)
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.PostResponse(
                    success=False,
                    message=f"Failed to unlike post: {str(e)}"
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.PostResponse(
                success=False,
                message=f"Failed to unlike post: {str(e)}"
            )

    def CreateComment(self, request, context):
        try:
            # First check if user exists
            user = self.db.query(User).filter(User.id == request.user_id).first()
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User with id {request.user_id} not found")
                return post_pb2.CommentResponse(success=False, message=f"User with id {request.user_id} not found")

            # Check if post exists
            post = self.repository.get_post(request.post_id)
            if not post:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Post with id {request.post_id} not found")
                return post_pb2.CommentResponse(success=False, message=f"Post with id {request.post_id} not found")

            try:
                # If parent_comment_id is 0, treat as None for top-level comment
                parent_comment_id = request.parent_comment_id if request.parent_comment_id > 0 else None

                comment = self.repository.create_comment(
                    post_id=request.post_id,
                    user_id=request.user_id,
                    comment_text=request.comment,
                    parent_comment_id=parent_comment_id
                )
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment created successfully",
                    comment=self._convert_to_proto_comment(comment),
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.CommentResponse(success=False, message=str(e))
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.CommentResponse(success=False, message=str(e))

    def UpdateComment(self, request, context):
        try:
            comment = self.repository.update_comment(
                comment_id=request.comment_id,
                comment_text=request.comment,
                status=request.status
            )
            if not comment:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found")
                return post_pb2.CommentResponse(success=False, message="Comment not found")

            return post_pb2.CommentResponse(
                success=True,
                message="Comment updated successfully",
                comment=self._convert_to_proto_comment(comment),
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.CommentResponse(success=False, message=str(e))

    def DeleteComment(self, request, context):
        try:
            success = self.repository.delete_comment(request.comment_id)
            if not success:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Comment not found")
                return post_pb2.GenericResponse(
                    success=False,
                    message="Comment not found"
                )

            return post_pb2.GenericResponse(
                success=True,
                message="Comment deleted successfully"
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.GenericResponse(
                success=False,
                message=f"Failed to delete comment: {str(e)}"
            )

    def GetComments(self, request, context):
        try:
            print(f"GetComments called with post_id: {request.post_id}, page: {request.page}, limit: {request.limit}")
            
            # Validate page number
            total_comments = self.repository.get_post_comment_count(request.post_id)
            print(f"Total comments for post {request.post_id}: {total_comments}")
            
            total_pages = (total_comments + request.limit - 1) // request.limit
            if total_pages == 0:
                total_pages = 1
            print(f"Total pages: {total_pages}")
            
            # If requested page is greater than total pages, return first page
            page = min(request.page, total_pages)
            if page < 1:
                page = 1
            print(f"Using page: {page}")

            comments, total = self.repository.get_comments(
                post_id=request.post_id,
                page=page,
                limit=request.limit
            )
            print(f"Retrieved {len(comments)} comments")
            
            # Debug print each comment
            for comment in comments:
                print(f"Comment ID: {comment.id}, User ID: {comment.user_id}, "
                      f"User: {comment.user.first_name if comment.user else 'None'} "
                      f"{comment.user.last_name if comment.user else 'None'}, "
                      f"Role: {comment.user.role if comment.user else 'None'}")
                print(f"Has {len(comment.replies)} replies")

            response = post_pb2.CommentListResponse(
                success=True,
                message="Comments retrieved successfully",
                comments=[self._convert_to_proto_comment(c) for c in comments],
                total_count=total,
                page=page,
                total_pages=total_pages
            )
            print("Successfully created CommentListResponse")
            return response
        except Exception as e:
            print(f"Error in GetComments: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.CommentListResponse(
                success=False,
                message=f"Failed to get comments: {str(e)}"
            )

    def LikeComment(self, request, context):
        try:
            # First check if user exists
            user = self.db.query(User).filter(User.id == request.user_id).first()
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User with id {request.user_id} not found")
                return post_pb2.CommentResponse(
                    success=False,
                    message=f"User with id {request.user_id} not found"
                )

            # Check if comment exists first
            comment = self.repository.get_comment(request.comment_id)
            if not comment:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Comment with id {request.comment_id} not found")
                return post_pb2.CommentResponse(
                    success=False,
                    message=f"Comment with id {request.comment_id} not found"
                )

            try:
                comment = self.repository.like_comment(
                    comment_id=request.comment_id,
                    user_id=request.user_id,
                    reaction_type=request.reaction_type
                )
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment liked successfully",
                    comment=self._convert_to_proto_comment(comment)
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.CommentResponse(
                    success=False,
                    message=f"Failed to like comment: {str(e)}"
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.CommentResponse(
                success=False,
                message=f"Failed to like comment: {str(e)}"
            )

    def UnlikeComment(self, request, context):
        try:
            # First check if user exists
            user = self.db.query(User).filter(User.id == request.user_id).first()
            if not user:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User with id {request.user_id} not found")
                return post_pb2.CommentResponse(
                    success=False,
                    message=f"User with id {request.user_id} not found"
                )

            # Check if comment exists first
            comment = self.repository.get_comment(request.comment_id)
            if not comment:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Comment with id {request.comment_id} not found")
                return post_pb2.CommentResponse(
                    success=False,
                    message=f"Comment with id {request.comment_id} not found"
                )

            try:
                comment = self.repository.unlike_comment(
                    comment_id=request.comment_id,
                    user_id=request.user_id
                )
                return post_pb2.CommentResponse(
                    success=True,
                    message="Comment unliked successfully",
                    comment=self._convert_to_proto_comment(comment)
                )
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return post_pb2.CommentResponse(
                    success=False,
                    message=f"Failed to unlike comment: {str(e)}"
                )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return post_pb2.CommentResponse(
                success=False,
                message=f"Failed to unlike comment: {str(e)}"
            )

def serve():
    port = os.getenv("PORT", "50055")
    engine = get_db_engine()
    # Import models so they register with Base.metadata before create_all
    from ..entity.post_entity import Post, PostLike, CommentLike  # noqa: F401
    from ..entity.comment_entity import Comment  # noqa: F401
    from ..entity.user_entity import User  # noqa: F401
    from ..entity.media_entity import media  # noqa: F401
    from ..utils.db_connection import Base

    try:
        Base.metadata.create_all(engine)
        print("Database tables verified!")
    except Exception as e:
        print(f"[WARN] Could not verify/create tables: {e}")

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=[AuthServerInterceptor()]
    )
    post_pb2_grpc.add_PostsServiceServicer_to_server(PostsService(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"Posts service started on port {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve() 
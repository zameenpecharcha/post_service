from __future__ import annotations

import os
from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import and_, desc, func, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from ..entity.comment_entity import Comment
from ..entity.media_entity import media as MediaTable
from ..entity.post_entity import CommentLike, Post, PostLike, PostShare
from ..entity.report_entity import Report
from ..utils.schema_helpers import (
    generate_business_code,
    normalize_comment_reaction,
    normalize_comment_status,
    normalize_post_reaction,
    normalize_post_status,
    normalize_post_type,
    normalize_reason_code,
    normalize_share_type,
    normalize_visibility,
    utcnow,
)
from ..utils.kafka_producer import publish_analytics_event, publish_comment_event, publish_post_event
from ..utils.log_utils import log_msg


class PostRepository:
    def __init__(self, db: Session):
        self.db = db

    def _active_posts(self):
        return self.db.query(Post).filter(Post.deleted_at.is_(None))

    def _active_comments(self):
        return self.db.query(Comment).filter(
            Comment.deleted_at.is_(None),
            Comment.status == "ACTIVE",
        )

    def _row_mapping(self, row) -> dict:
        return dict(row._mapping) if hasattr(row, "_mapping") else {}

    def _post_thumbnail_url(self, post_id: UUID) -> str:
        rows = self.get_post_media(post_id)
        if not rows:
            return ""
        for row in rows:
            media = self._row_mapping(row)
            if media.get("thumbnail_url"):
                return media["thumbnail_url"]
        first = self._row_mapping(rows[0])
        return first.get("thumbnail_url") or first.get("file_url") or ""

    def _publish_post_event(self, event_type: str, post: Post) -> None:
        if not post:
            log_msg("warning", f"Skip {event_type}: post is missing")
            return
        try:
            sent = publish_post_event(event_type, post, thumbnail_url=self._post_thumbnail_url(post.id))
            if sent:
                log_msg("info", f"Triggered {event_type} for post_id={post.id}")
            else:
                log_msg("error", f"{event_type} was not sent to Kafka for post_id={post.id}")
            # Mirror lifecycle into ClickHouse (OpenSearch gets the domain event above).
            if event_type in {"POST_CREATED", "POST_UPDATED", "POST_DELETED"}:
                self._publish_post_analytics(event_type, post, user_id=getattr(post, "user_id", None))
        except Exception as exc:
            log_msg("error", f"Failed to publish {event_type} for post_id={getattr(post, 'id', None)}: {exc}")

    def _comment_property_id(self, comment: Comment) -> Optional[str]:
        if not comment or not getattr(comment, "post_id", None):
            return None
        post = self.db.query(Post).filter(Post.id == comment.post_id).first()
        return str(post.property_id) if post and post.property_id else None

    def _publish_comment_event(self, event_type: str, comment: Comment) -> None:
        if not comment:
            log_msg("warning", f"Skip {event_type}: comment is missing")
            return
        try:
            sent = publish_comment_event(
                event_type,
                comment,
                property_id=self._comment_property_id(comment),
            )
            if sent:
                log_msg("info", f"Triggered {event_type} for comment_id={comment.id}")
            else:
                log_msg("error", f"{event_type} was not sent to Kafka for comment_id={comment.id}")
            if event_type in {"COMMENT_CREATED", "COMMENT_UPDATED", "COMMENT_DELETED"}:
                self._publish_comment_analytics(
                    event_type,
                    comment,
                    user_id=getattr(comment, "user_id", None),
                )
        except Exception as exc:
            log_msg("error", f"Failed to publish {event_type} for comment_id={getattr(comment, 'id', None)}: {exc}")

    def _post_analytics_payload(self, post: Post, user_id: UUID = None, view_duration: int = 0) -> dict:
        location = getattr(post, "location", None) or ""
        city = location.split(",")[-1].strip() if location else ""
        return {
            "userId": str(user_id) if user_id else "",
            "postId": str(post.id) if post and post.id else "",
            "postCode": getattr(post, "post_code", None) or (f"POST-{post.id}" if post and post.id else ""),
            "createdBy": str(post.user_id) if post and post.user_id else "",
            "actorUserId": str(user_id) if user_id else "",
            "targetUserId": str(post.user_id) if post and post.user_id else "",
            "postTitle": getattr(post, "title", None) or "",
            "city": city,
            "visibility": getattr(post, "visibility", None) or "PUBLIC",
            "viewDuration": int(view_duration or 0),
        }

    def _comment_analytics_payload(self, comment: Comment, user_id: UUID = None) -> dict:
        parent_id = getattr(comment, "parent_comment_id", None)
        actor_id = user_id or getattr(comment, "user_id", None)
        return {
            "userId": str(actor_id) if actor_id else "",
            "commentId": str(comment.id) if comment and comment.id else "",
            "postId": str(comment.post_id) if comment and comment.post_id else "",
            "parentCommentId": str(parent_id) if parent_id else None,
        }

    def _publish_post_analytics(self, event_type: str, post: Post, user_id: UUID = None, view_duration: int = 0) -> None:
        if not post:
            log_msg("warning", f"Skip {event_type}: post is missing")
            return
        try:
            sent = publish_analytics_event(
                event_type,
                self._post_analytics_payload(post, user_id, view_duration),
                key=str(post.id),
                topic=os.getenv("KAFKA_POST_EVENTS_TOPIC", "post-events"),
            )
            if sent:
                log_msg("info", f"Triggered {event_type} analytics for post_id={post.id} user_id={user_id}")
            else:
                log_msg("error", f"{event_type} analytics was not sent to Kafka for post_id={post.id}")
        except Exception as exc:
            log_msg("error", f"Failed to publish post analytics {event_type}: {exc}")

    def _publish_comment_analytics(self, event_type: str, comment: Comment, user_id: UUID = None) -> None:
        if not comment:
            log_msg("warning", f"Skip {event_type}: comment is missing")
            return
        try:
            sent = publish_analytics_event(
                event_type,
                self._comment_analytics_payload(comment, user_id),
                key=str(comment.id),
                topic=os.getenv("KAFKA_COMMENTS_EVENTS_TOPIC", "comments-events"),
            )
            if sent:
                log_msg("info", f"Triggered {event_type} analytics for comment_id={comment.id} user_id={user_id}")
            else:
                log_msg("error", f"{event_type} analytics was not sent to Kafka for comment_id={comment.id}")
        except Exception as exc:
            log_msg("error", f"Failed to publish comment analytics {event_type}: {exc}")

    def _publish_property_analytics(self, event_type: str, property_id: UUID, user_id: UUID = None) -> None:
        if not property_id:
            return
        try:
            publish_analytics_event(
                event_type,
                {
                    "userId": str(user_id) if user_id else "",
                    "propertyId": str(property_id),
                    "propertyCode": "",
                    "builderName": "",
                    "projectName": "",
                    "propertyType": "",
                    "listingType": "",
                    "city": "",
                    "state": "",
                    "viewDuration": 0,
                },
                key=str(property_id),
                topic=os.getenv("KAFKA_PROPERTY_EVENTS_TOPIC", "property-events"),
            )
        except Exception as orig_exc:
            log_msg("error", f"Failed to publish property analytics {event_type}: {orig_exc}")

    # ------------------------------------------------------------------ posts
    def create_post(
        self,
        user_id: UUID,
        title: str,
        content: str,
        visibility: str = None,
        post_type: str = None,
        location: str = None,
        price: float = None,
        status: str = None,
        is_anonymous: bool = False,
        latitude: float = None,
        longitude: float = None,
        property_id: UUID = None,
        currency: str = "INR",
        commit: bool = True,
    ) -> Post:
        try:
            post = Post(
                post_code=generate_business_code("PST"),
                user_id=user_id,
                title=title,
                content=content,
                visibility=normalize_visibility(visibility),
                post_type=normalize_post_type(post_type),
                location=location,
                latitude=latitude,
                longitude=longitude,
                price=price,
                currency=currency or "INR",
                status=normalize_post_status(status),
                is_anonymous=is_anonymous,
                property_id=property_id,
                created_at=utcnow(),
                updated_at=utcnow(),
            )
            self.db.add(post)
            if commit:
                self.db.commit()
                self.db.refresh(post)
                self._publish_post_event("POST_CREATED", post)
            else:
                self.db.flush()
            return post
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while creating post: {e}") from e

    def get_post(self, post_id: UUID) -> Optional[Post]:
        try:
            return self._active_posts().filter(Post.id == post_id).first()
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching post: {e}") from e

    def record_post_view(self, post_id: UUID, user_id: UUID = None, view_duration: int = 0) -> Optional[Post]:
        post = self.get_post(post_id)
        if not post:
            return None
        post.view_count = int(post.view_count or 0) + 1
        post.updated_at = utcnow()
        self.db.commit()
        self.db.refresh(post)
        self._publish_post_analytics("POST_VIEWED", post, user_id=user_id, view_duration=view_duration)
        return post

    def update_post(
        self,
        post_id: UUID,
        title: str = None,
        content: str = None,
        visibility: str = None,
        post_type: str = None,
        type: str = None,
        location: str = None,
        price: float = None,
        status: str = None,
        is_anonymous: bool = None,
        latitude: float = None,
        longitude: float = None,
        property_id: UUID = None,
        currency: str = None,
    ) -> Optional[Post]:
        try:
            post = self.get_post(post_id)
            if not post:
                return None

            if title is not None:
                post.title = title
            if content is not None:
                post.content = content
            if visibility is not None:
                post.visibility = normalize_visibility(visibility)
            effective_type = post_type if post_type is not None else type
            if effective_type is not None:
                post.post_type = normalize_post_type(effective_type)
            if location is not None:
                post.location = location
            if latitude is not None:
                post.latitude = latitude
            if longitude is not None:
                post.longitude = longitude
            if price is not None:
                post.price = price
            if status is not None:
                post.status = normalize_post_status(status)
            if is_anonymous is not None:
                post.is_anonymous = is_anonymous
            if property_id is not None:
                post.property_id = property_id
            if currency is not None:
                post.currency = currency

            post.updated_at = utcnow()
            self.db.add(post)
            self.db.commit()
            self.db.refresh(post)
            # Soft-delete via status should remove from OpenSearch, not re-index as DELETED.
            if normalize_post_status(post.status) == "DELETED":
                self._publish_post_event("POST_DELETED", post)
            else:
                self._publish_post_event("POST_UPDATED", post)
            return post
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while updating post: {e}") from e

    def delete_post(self, post_id: UUID) -> bool:
        try:
            post = self.db.query(Post).filter(Post.id == post_id, Post.deleted_at.is_(None)).first()
            if not post:
                return False

            now = utcnow()
            post.status = "DELETED"
            post.deleted_at = now
            post.updated_at = now
            self.db.execute(
                update(MediaTable)
                .where(
                    and_(
                        MediaTable.c.entity_id == post_id,
                        MediaTable.c.entity_type == "POST",
                    )
                )
                .values(status="DELETED", updated_at=now)
            )
            self.db.commit()
            self._publish_post_event("POST_DELETED", post)
            return True
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while deleting post: {e}") from e

    def get_posts_by_user(self, user_id: UUID, page: int = 1, limit: int = 10) -> Tuple[List[Post], int]:
        try:
            query = self._active_posts().filter(Post.user_id == user_id)
            total = query.count()
            posts = (
                query.order_by(desc(Post.is_pinned), desc(Post.pinned_at), desc(Post.created_at))
                .offset((page - 1) * limit)
                .limit(limit)
                .all()
            )
            return posts, total
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching user posts: {e}") from e

    def search_posts(
        self,
        type: str = None,
        location: str = None,
        min_price: float = None,
        max_price: float = None,
        status: str = None,
        query: str = None,
        hashtag: str = None,
        page: int = 1,
        limit: int = 10,
    ) -> Tuple[List[Post], int]:
        try:
            query_obj = self._active_posts()

            if type and str(type).strip():
                query_obj = query_obj.filter(Post.post_type == normalize_post_type(type))
            if location and str(location).strip():
                query_obj = query_obj.filter(Post.location.ilike(f"%{location}%"))
            if min_price is not None and min_price > 0:
                query_obj = query_obj.filter(Post.price >= min_price)
            if max_price is not None and max_price > 0:
                query_obj = query_obj.filter(Post.price <= max_price)
            if status and str(status).strip():
                query_obj = query_obj.filter(Post.status == normalize_post_status(status))

            keyword = (query or "").strip()
            tag = (hashtag or "").strip().lstrip("#")
            if keyword:
                pattern = f"%{keyword}%"
                query_obj = query_obj.filter(
                    (Post.title.ilike(pattern)) | (Post.content.ilike(pattern))
                )
            if tag:
                tag_pattern = f"%#{tag}%"
                query_obj = query_obj.filter(Post.content.ilike(tag_pattern))

            total = query_obj.count()
            posts = (
                query_obj.order_by(desc(Post.created_at))
                .offset((page - 1) * limit)
                .limit(limit)
                .all()
            )
            return posts, total
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while searching posts: {e}") from e

    def get_public_posts(self, page: int = 1, limit: int = 10) -> Tuple[List[Post], int]:
        try:
            query = (
                self._active_posts()
                .filter(Post.visibility == "PUBLIC", Post.status == "PUBLISHED")
            )
            total = query.count()
            posts = (
                query.order_by(desc(Post.created_at))
                .offset((page - 1) * limit)
                .limit(limit)
                .all()
            )
            return posts, total
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching public posts: {e}") from e

    def get_property_posts(
        self, property_id: UUID, page: int = 1, limit: int = 10
    ) -> Tuple[List[Post], int]:
        try:
            query = (
                self._active_posts()
                .filter(Post.property_id == property_id, Post.status == "PUBLISHED")
            )
            total = query.count()
            posts = (
                query.order_by(desc(Post.created_at))
                .offset((page - 1) * limit)
                .limit(limit)
                .all()
            )
            return posts, total
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching property posts: {e}") from e

    def get_builder_posts(
        self,
        builder_user_id: UUID = None,
        user_ids: List[UUID] = None,
        page: int = 1,
        limit: int = 10,
    ) -> Tuple[List[Post], int]:
        try:
            query = self._active_posts().filter(Post.status == "PUBLISHED")
            if builder_user_id:
                query = query.filter(Post.user_id == builder_user_id)
            elif user_ids:
                query = query.filter(Post.user_id.in_(user_ids))
            else:
                return [], 0
            total = query.count()
            posts = (
                query.order_by(desc(Post.created_at))
                .offset((page - 1) * limit)
                .limit(limit)
                .all()
            )
            return posts, total
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching builder posts: {e}") from e

    def pin_post(self, post_id: UUID, user_id: UUID) -> Optional[Post]:
        post = self.db.query(Post).filter(Post.id == post_id, Post.deleted_at.is_(None)).first()
        if not post or post.user_id != user_id:
            return None
        now = utcnow()
        (
            self.db.query(Post)
            .filter(Post.user_id == user_id, Post.is_pinned.is_(True), Post.id != post_id)
            .update({"is_pinned": False, "pinned_at": None, "updated_at": now}, synchronize_session=False)
        )
        post.is_pinned = True
        post.pinned_at = now
        post.updated_at = now
        self.db.commit()
        self.db.refresh(post)
        return post

    def unpin_post(self, post_id: UUID, user_id: UUID) -> Optional[Post]:
        post = self.db.query(Post).filter(Post.id == post_id, Post.deleted_at.is_(None)).first()
        if not post or post.user_id != user_id:
            return None
        post.is_pinned = False
        post.pinned_at = None
        post.updated_at = utcnow()
        self.db.commit()
        self.db.refresh(post)
        return post

    def archive_post(self, post_id: UUID, user_id: UUID) -> Optional[Post]:
        post = self.db.query(Post).filter(Post.id == post_id, Post.deleted_at.is_(None)).first()
        if not post or post.user_id != user_id:
            return None
        post.status = "ARCHIVED"
        post.updated_at = utcnow()
        self.db.commit()
        self.db.refresh(post)
        self._publish_post_event("POST_UPDATED", post)
        return post

    def restore_archived_post(self, post_id: UUID, user_id: UUID) -> Optional[Post]:
        post = self.db.query(Post).filter(Post.id == post_id, Post.deleted_at.is_(None)).first()
        if not post or post.user_id != user_id:
            return None
        post.status = "PUBLISHED"
        post.updated_at = utcnow()
        self.db.commit()
        self.db.refresh(post)
        self._publish_post_event("POST_UPDATED", post)
        return post

    def get_trending_posts(self, limit: int = 10) -> List[Post]:
        try:
            return (
                self._active_posts()
                .order_by(desc(Post.like_count), desc(Post.created_at))
                .limit(limit)
                .all()
            )
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while fetching trending posts: {e}") from e

    def get_liked_post_ids(self, user_id: UUID, post_ids: List[UUID]) -> set:
        if not user_id or not post_ids:
            return set()
        rows = (
            self.db.query(PostLike.post_id)
            .filter(PostLike.user_id == user_id, PostLike.post_id.in_(post_ids))
            .all()
        )
        return {r[0] for r in rows}

    # ------------------------------------------------------------------ media
    def add_post_media(
        self,
        post_id: UUID,
        uploaded_by: UUID,
        media_type: str,
        file_url: str,
        display_order: int,
        file_size: int = 0,
        file_name: str = None,
        mime_type: str = None,
        commit: bool = True,
    ) -> UUID:
        try:
            now = utcnow()
            insert_stmt = (
                MediaTable.insert()
                .returning(MediaTable.c.id)
                .values(
                    media_code=generate_business_code("MED"),
                    entity_type="POST",
                    entity_id=post_id,
                    uploaded_by=uploaded_by,
                    media_type=media_type,
                    file_name=file_name,
                    file_url=file_url,
                    mime_type=mime_type,
                    file_size=file_size,
                    display_order=display_order,
                    status="ACTIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            result = self.db.execute(insert_stmt)
            if commit:
                self.db.commit()
            else:
                self.db.flush()
            post = self.get_post(post_id)
            if post:
                self._publish_post_event("POST_UPDATED", post)
            return result.scalar()
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while adding media: {e}") from e

    def delete_post_media(self, media_id: UUID) -> bool:
        try:
            media_row = self.db.execute(
                select(MediaTable).where(MediaTable.c.id == media_id)
            ).fetchone()
            post_id = self._row_mapping(media_row).get("entity_id") if media_row else None
            now = utcnow()
            result = self.db.execute(
                update(MediaTable)
                .where(MediaTable.c.id == media_id)
                .values(status="DELETED", updated_at=now)
            )
            self.db.commit()
            if result.rowcount > 0 and post_id:
                post = self.db.query(Post).filter(Post.id == post_id).first()
                if post:
                    self._publish_post_event("POST_UPDATED", post)
            return result.rowcount > 0
        except SQLAlchemyError:
            self.db.rollback()
            return False

    def update_media_url_size(
        self,
        media_id: UUID,
        file_url: str,
        file_size: int,
        commit: bool = True,
    ) -> bool:
        try:
            result = self.db.execute(
                update(MediaTable)
                .where(MediaTable.c.id == media_id)
                .values(file_url=file_url, file_size=file_size, updated_at=utcnow())
            )
            if commit:
                self.db.commit()
            return result.rowcount > 0
        except SQLAlchemyError:
            self.db.rollback()
            return False

    def get_post_media(self, post_id: UUID):
        try:
            stmt = (
                select(MediaTable)
                .where(
                    and_(
                        MediaTable.c.entity_id == post_id,
                        MediaTable.c.entity_type == "POST",
                        func.coalesce(MediaTable.c.status, "ACTIVE") != "DELETED",
                    )
                )
                .order_by(MediaTable.c.display_order)
            )
            return self.db.execute(stmt).fetchall()
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching media: {e}") from e

    def get_posts_media_map(self, post_ids: List[UUID]) -> dict:
        if not post_ids:
            return {}
        try:
            stmt = (
                select(MediaTable)
                .where(
                    and_(
                        MediaTable.c.entity_id.in_(post_ids),
                        MediaTable.c.entity_type == "POST",
                        func.coalesce(MediaTable.c.status, "ACTIVE") != "DELETED",
                    )
                )
                .order_by(MediaTable.c.entity_id, MediaTable.c.display_order)
            )
            rows = self.db.execute(stmt).fetchall()
            out = {pid: [] for pid in post_ids}
            for row in rows:
                mapping = getattr(row, "_mapping", None) or {}
                pid = mapping.get("entity_id") if mapping else getattr(row, "entity_id", None)
                if pid is not None:
                    out.setdefault(pid, []).append(row)
            return out
        except SQLAlchemyError as e:
            raise Exception(f"Database error while batch-fetching media: {e}") from e

    # ------------------------------------------------------------------ likes
    def like_post(self, post_id: UUID, user_id: UUID, reaction_type: str = "LIKE") -> Optional[Post]:
        try:
            post = self.get_post(post_id)
            if not post:
                raise Exception(f"Post with ID {post_id} not found")

            existing_like = (
                self.db.query(PostLike)
                .filter(PostLike.post_id == post_id, PostLike.user_id == user_id)
                .first()
            )

            if not existing_like:
                try:
                    like = PostLike(
                        post_id=post_id,
                        user_id=user_id,
                        reaction_type=normalize_post_reaction(reaction_type),
                        created_at=utcnow(),
                    )
                    self.db.add(like)
                    post.like_count = (post.like_count or 0) + 1
                    post.updated_at = utcnow()
                    self.db.commit()
                    log_msg("info", f"like_post saved, publishing POST_LIKED post_id={post_id} user_id={user_id}")
                    self._publish_post_analytics("POST_LIKED", post, user_id=user_id)
                except IntegrityError:
                    self.db.rollback()
                    log_msg("warning", f"like_post skipped, like already exists post_id={post_id} user_id={user_id}")
                except SQLAlchemyError as e:
                    self.db.rollback()
                    raise Exception(f"Database error while adding like: {e}") from e
            elif reaction_type:
                existing_like.reaction_type = normalize_post_reaction(reaction_type)
                self.db.commit()
                log_msg("info", f"like_post already liked, no POST_LIKED event post_id={post_id} user_id={user_id}")

            self.db.refresh(post)
            return post
        except Exception:
            self.db.rollback()
            raise

    def unlike_post(self, post_id: UUID, user_id: UUID) -> Optional[Post]:
        try:
            post = self.get_post(post_id)
            if not post:
                raise Exception(f"Post with ID {post_id} not found")

            like = (
                self.db.query(PostLike)
                .filter(PostLike.post_id == post_id, PostLike.user_id == user_id)
                .first()
            )
            if like:
                self.db.delete(like)
                post.like_count = max(0, (post.like_count or 0) - 1)
                post.updated_at = utcnow()
                self.db.commit()
                self._publish_post_analytics("POST_UNLIKED", post, user_id=user_id)

            self.db.refresh(post)
            return post
        except Exception:
            self.db.rollback()
            raise

    # ---------------------------------------------------------------- comments
    def create_comment(
        self,
        post_id: UUID,
        user_id: UUID,
        content: str,
        parent_comment_id: UUID = None,
        is_anonymous: bool = False,
    ) -> Comment:
        try:
            post = self.get_post(post_id)
            if not post:
                raise Exception(f"Post with ID {post_id} not found")
            if not post.allow_comments:
                raise Exception("Comments are disabled for this post")

            if parent_comment_id:
                parent_comment = self.get_comment(parent_comment_id)
                if not parent_comment:
                    raise Exception(f"Parent comment with ID {parent_comment_id} not found")
                if parent_comment.post_id != post_id:
                    raise Exception("Parent comment does not belong to the specified post")

            now = utcnow()
            comment = Comment(
                post_id=post_id,
                user_id=user_id,
                content=content,
                parent_comment_id=parent_comment_id,
                is_anonymous=is_anonymous,
                status="ACTIVE",
                created_at=now,
                updated_at=now,
            )
            self.db.add(comment)

            if parent_comment_id:
                parent = self.get_comment(parent_comment_id)
                if parent:
                    parent.reply_count = (parent.reply_count or 0) + 1
                    parent.updated_at = now
            else:
                post.comment_count = (post.comment_count or 0) + 1
                post.updated_at = now

            self.db.commit()
            self.db.refresh(comment)
            log_msg("info", f"create_comment saved, publishing COMMENT_CREATED comment_id={comment.id} post_id={post_id}")
            self._publish_comment_event("COMMENT_CREATED", comment)
            self._publish_post_analytics("POST_COMMENTED", post, user_id=user_id)
            return comment
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while creating comment: {e}") from e

    def get_comment(self, comment_id: UUID) -> Optional[Comment]:
        return (
            self._active_comments()
            .filter(Comment.id == comment_id)
            .first()
        )

    def update_comment(
        self,
        comment_id: UUID,
        content: str = None,
        status: str = None,
    ) -> Optional[Comment]:
        comment = self.db.query(Comment).filter(Comment.id == comment_id).first()
        if not comment:
            return None
        if content is not None:
            comment.content = content
        if status is not None:
            comment.status = normalize_comment_status(status)
        comment.updated_at = utcnow()
        self.db.commit()
        self.db.refresh(comment)
        self._publish_comment_event("COMMENT_UPDATED", comment)
        return comment

    def delete_comment(self, comment_id: UUID) -> bool:
        comment = self.db.query(Comment).filter(Comment.id == comment_id, Comment.deleted_at.is_(None)).first()
        if not comment:
            return False

        now = utcnow()
        comment.status = "DELETED"
        comment.deleted_at = now
        comment.updated_at = now

        post = self.db.query(Post).filter(Post.id == comment.post_id).first()
        if post:
            if comment.parent_comment_id:
                parent = self.db.query(Comment).filter(Comment.id == comment.parent_comment_id).first()
                if parent:
                    parent.reply_count = max(0, (parent.reply_count or 0) - 1)
            else:
                post.comment_count = max(0, (post.comment_count or 0) - 1)
            post.updated_at = now

        self.db.commit()
        self._publish_comment_event("COMMENT_DELETED", comment)
        return True

    def get_comments(self, post_id: UUID, page: int = 1, limit: int = 10) -> Tuple[List[Comment], int]:
        try:
            # Load every active comment for the post, then build a full reply tree.
            # (joinedload only loads one level — nested replies otherwise disappear.)
            all_comments = (
                self._active_comments()
                .filter(Comment.post_id == post_id)
                .order_by(Comment.created_at.asc())
                .all()
            )

            children_map: dict = {}
            roots: List[Comment] = []
            for comment in all_comments:
                parent_id = comment.parent_comment_id
                if parent_id:
                    children_map.setdefault(parent_id, []).append(comment)
                else:
                    roots.append(comment)

            roots.sort(key=lambda c: c.created_at or 0, reverse=True)
            total = len(roots)
            page_roots = roots[(page - 1) * limit : (page - 1) * limit + limit]

            def attach_thread(node: Comment) -> None:
                kids = sorted(
                    children_map.get(node.id, []),
                    key=lambda c: c.created_at or 0,
                )
                object.__setattr__(node, "_thread_replies", kids)
                for kid in kids:
                    attach_thread(kid)

            for root in page_roots:
                attach_thread(root)

            return page_roots, total
        except SQLAlchemyError as e:
            raise Exception(f"Database error while getting comments: {e}") from e

    def like_comment(self, comment_id: UUID, user_id: UUID, reaction_type: str = "LIKE") -> Optional[Comment]:
        try:
            comment = self.get_comment(comment_id)
            if not comment:
                raise Exception(f"Comment with ID {comment_id} not found")

            existing_like = (
                self.db.query(CommentLike)
                .filter(CommentLike.comment_id == comment_id, CommentLike.user_id == user_id)
                .first()
            )

            if not existing_like:
                try:
                    like = CommentLike(
                        comment_id=comment_id,
                        user_id=user_id,
                        reaction_type=normalize_comment_reaction(reaction_type),
                        created_at=utcnow(),
                    )
                    self.db.add(like)
                    comment.like_count = (comment.like_count or 0) + 1
                    comment.updated_at = utcnow()
                    self.db.commit()
                    self._publish_comment_analytics("COMMENT_LIKED", comment, user_id=user_id)
                except IntegrityError:
                    self.db.rollback()
                except SQLAlchemyError as e:
                    self.db.rollback()
                    raise Exception(f"Database error while adding like: {e}") from e
            elif reaction_type:
                existing_like.reaction_type = normalize_comment_reaction(reaction_type)
                self.db.commit()

            self.db.refresh(comment)
            return comment
        except Exception:
            self.db.rollback()
            raise

    def unlike_comment(self, comment_id: UUID, user_id: UUID) -> Optional[Comment]:
        try:
            comment = self.get_comment(comment_id)
            if not comment:
                raise Exception(f"Comment with ID {comment_id} not found")

            like = (
                self.db.query(CommentLike)
                .filter(CommentLike.comment_id == comment_id, CommentLike.user_id == user_id)
                .first()
            )
            if like:
                self.db.delete(like)
                comment.like_count = max(0, (comment.like_count or 0) - 1)
                comment.updated_at = utcnow()
                self.db.commit()

            self.db.refresh(comment)
            return comment
        except Exception:
            self.db.rollback()
            raise

    # ---------------------------------------------------------------- helpers
    def get_post_like_count(self, post_id: UUID) -> int:
        post = self.db.query(Post.like_count).filter(Post.id == post_id).first()
        return int(post[0]) if post else 0

    def get_comment_like_count(self, comment_id: UUID) -> int:
        comment = self.db.query(Comment.like_count).filter(Comment.id == comment_id).first()
        return int(comment[0]) if comment else 0

    def get_post_comment_count(self, post_id: UUID) -> int:
        post = self.db.query(Post.comment_count).filter(Post.id == post_id).first()
        return int(post[0]) if post else 0

    def get_posts_like_counts(self, post_ids: List[UUID]) -> dict:
        if not post_ids:
            return {}
        rows = self.db.query(Post.id, Post.like_count).filter(Post.id.in_(post_ids)).all()
        return {pid: int(cnt or 0) for pid, cnt in rows}

    def get_posts_comment_counts(self, post_ids: List[UUID]) -> dict:
        if not post_ids:
            return {}
        rows = self.db.query(Post.id, Post.comment_count).filter(Post.id.in_(post_ids)).all()
        return {pid: int(cnt or 0) for pid, cnt in rows}

    def get_comment_with_user(self, comment_id: UUID) -> Optional[Comment]:
        return self.get_comment(comment_id)

    def get_replies(self, comment_id: UUID, page: int = 1, limit: int = 10) -> Tuple[List[Comment], int]:
        query = (
            self._active_comments()
            .filter(Comment.parent_comment_id == comment_id)
            .order_by(desc(Comment.created_at))
        )
        total = query.count()
        replies = query.offset((page - 1) * limit).limit(limit).all()
        return replies, total

    def get_post_likes(self, post_id: UUID, page: int = 1, limit: int = 10) -> Tuple[List[PostLike], int]:
        query = (
            self.db.query(PostLike)
            .filter(PostLike.post_id == post_id)
            .order_by(desc(PostLike.created_at))
        )
        total = query.count()
        likes = query.offset((page - 1) * limit).limit(limit).all()
        return likes, total

    def check_like_status(self, post_id: UUID, user_id: UUID) -> Tuple[bool, Optional[str]]:
        like = (
            self.db.query(PostLike)
            .filter(PostLike.post_id == post_id, PostLike.user_id == user_id)
            .first()
        )
        if not like:
            return False, None
        return True, like.reaction_type

    def share_post(
        self,
        post_id: UUID,
        shared_by: UUID,
        share_type: str = "SHARE",
        caption: str = None,
        visibility: str = "PUBLIC",
    ) -> Optional[PostShare]:
        post = self.get_post(post_id)
        if not post or not post.allow_share:
            return None
        now = utcnow()
        share = PostShare(
            share_code=generate_business_code("SHR"),
            post_id=post_id,
            shared_by=shared_by,
            share_type=normalize_share_type(share_type),
            caption=caption,
            visibility=normalize_visibility(visibility),
            created_at=now,
        )
        self.db.add(share)
        post.share_count = (post.share_count or 0) + 1
        post.updated_at = now
        self.db.commit()
        self.db.refresh(share)
        self._publish_post_analytics("POST_SHARED", post, user_id=shared_by)
        return share

    def get_shared_posts(self, user_id: UUID, page: int = 1, limit: int = 10) -> Tuple[List[PostShare], int]:
        query = (
            self.db.query(PostShare)
            .filter(PostShare.shared_by == user_id)
            .order_by(desc(PostShare.created_at))
        )
        total = query.count()
        shares = query.offset((page - 1) * limit).limit(limit).all()
        return shares, total

    def delete_shared_post(self, share_id: UUID, user_id: UUID) -> bool:
        share = self.db.query(PostShare).filter(PostShare.id == share_id).first()
        if not share or share.shared_by != user_id:
            return False
        post = self.db.query(Post).filter(Post.id == share.post_id).first()
        if post:
            post.share_count = max(0, (post.share_count or 0) - 1)
            post.updated_at = utcnow()
        self.db.delete(share)
        self.db.commit()
        return True

    def create_report(
        self,
        entity_type: str,
        entity_id: UUID,
        reported_by: UUID,
        reported_user_id: UUID = None,
        reason_code: str = None,
        description: str = None,
    ) -> Report:
        now = utcnow()
        report = Report(
            report_code=generate_business_code("REP"),
            entity_type=entity_type,
            entity_id=entity_id,
            reported_by=reported_by,
            reported_user_id=reported_user_id,
            reason_code=normalize_reason_code(reason_code),
            description=description,
            status="PENDING",
            priority="MEDIUM",
            created_at=now,
            updated_at=now,
        )
        self.db.add(report)

        if entity_type == "POST":
            post = self.db.query(Post).filter(Post.id == entity_id).first()
            if post:
                post.report_count = (post.report_count or 0) + 1
                post.updated_at = now
        elif entity_type == "COMMENT":
            comment = self.db.query(Comment).filter(Comment.id == entity_id).first()
            if comment:
                comment.report_count = (comment.report_count or 0) + 1
                comment.updated_at = now

        self.db.commit()
        self.db.refresh(report)
        if entity_type == "POST":
            post = self.db.query(Post).filter(Post.id == entity_id).first()
            self._publish_post_analytics("POST_REPORTED", post, user_id=reported_by)
        elif entity_type == "COMMENT":
            comment = self.db.query(Comment).filter(Comment.id == entity_id).first()
            self._publish_comment_analytics("COMMENT_REPORTED", comment, user_id=reported_by)
        elif entity_type == "PROPERTY":
            self._publish_property_analytics("PROPERTY_REPORTED", entity_id, user_id=reported_by)
        return report

    def get_reports_by_user(self, reported_by: UUID, page: int = 1, limit: int = 10) -> Tuple[List[Report], int]:
        query = (
            self.db.query(Report)
            .filter(Report.reported_by == reported_by)
            .order_by(desc(Report.created_at))
        )
        total = query.count()
        reports = query.offset((page - 1) * limit).limit(limit).all()
        return reports, total

    def get_report(self, report_id: UUID) -> Optional[Report]:
        return self.db.query(Report).filter(Report.id == report_id).first()

    def get_reports(
        self,
        status: str = None,
        entity_type: str = None,
        priority: str = None,
        reported_by: UUID = None,
        reported_user_id: UUID = None,
        entity_id: UUID = None,
        page: int = 1,
        limit: int = 20,
    ) -> Tuple[List[Report], int]:
        query = self.db.query(Report)
        if status and str(status).strip():
            query = query.filter(Report.status == str(status).strip().upper())
        if entity_type and str(entity_type).strip():
            query = query.filter(Report.entity_type == str(entity_type).strip().upper())
        if priority and str(priority).strip():
            query = query.filter(Report.priority == str(priority).strip().upper())
        if reported_by:
            query = query.filter(Report.reported_by == reported_by)
        if reported_user_id:
            query = query.filter(Report.reported_user_id == reported_user_id)
        if entity_id:
            query = query.filter(Report.entity_id == entity_id)
        query = query.order_by(desc(Report.created_at))
        total = query.count()
        reports = query.offset((page - 1) * limit).limit(limit).all()
        return reports, total

    def update_report_status(
        self,
        report_id: UUID,
        status: str,
        reviewed_by: UUID = None,
        action_taken: str = None,
        action_note: str = None,
        priority: str = None,
    ) -> Optional[Report]:
        report = self.get_report(report_id)
        if not report:
            return None
        now = utcnow()
        report.status = str(status).strip().upper()
        if reviewed_by:
            report.reviewed_by = reviewed_by
        if action_taken is not None:
            report.action_taken = action_taken
        if action_note is not None:
            report.action_note = action_note
        if priority:
            report.priority = str(priority).strip().upper()
        report.reviewed_at = now
        report.updated_at = now
        self.db.commit()
        self.db.refresh(report)
        return report

    def assign_report(self, report_id: UUID, reviewed_by: UUID) -> Optional[Report]:
        report = self.get_report(report_id)
        if not report:
            return None
        now = utcnow()
        report.reviewed_by = reviewed_by
        if (report.status or "").upper() == "PENDING":
            report.status = "UNDER_REVIEW"
        report.updated_at = now
        self.db.commit()
        self.db.refresh(report)
        return report

    def get_report_stats(self) -> dict:
        def _count(status: str = None, priority: str = None) -> int:
            q = self.db.query(func.count(Report.id))
            if status:
                q = q.filter(Report.status == status)
            if priority:
                q = q.filter(Report.priority == priority)
            return int(q.scalar() or 0)

        return {
            "pending_count": _count("PENDING"),
            "under_review_count": _count("UNDER_REVIEW"),
            "resolved_count": _count("RESOLVED"),
            "rejected_count": _count("REJECTED"),
            "high_priority_count": _count(priority="HIGH"),
            "total_count": _count(),
        }

    def delete_report(self, report_id: UUID) -> bool:
        report = self.get_report(report_id)
        if not report:
            return False
        self.db.delete(report)
        self.db.commit()
        return True

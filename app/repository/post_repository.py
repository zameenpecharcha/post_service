from typing import List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import desc, func, select, and_, update
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from ..entity.post_entity import Post, PostLike, CommentLike
from ..entity.media_entity import media as MediaTable
from ..entity.comment_entity import Comment
from ..entity.user_entity import User
import sqlalchemy.orm

class PostRepository:
    def __init__(self, db: Session):
        self.db = db

    # Post Operations
    def create_post(self, user_id: int, title: str, content: str, visibility: str = None,
                   type: str = None, location: str = None, map_location: str = None,
                   price: float = None, status: str = 'active', is_anonymous: bool = False,
                   latitude: float = None, longitude: float = None,
                   commit: bool = True) -> Post:
        try:
            post = Post(
                user_id=user_id,
                title=title,
                content=content,
                visibility=visibility,
                type=type,
                location=location,
                latitude=latitude,
                longitude=longitude,
                price=price,
                status=status,
                is_anonymous=is_anonymous,
                created_at=datetime.utcnow()
            )
            self.db.add(post)
            if commit:
                self.db.commit()
                self.db.refresh(post)
            else:
                # Ensure PK is generated without committing
                self.db.flush()
            return post
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while creating post: {str(e)}")

    def get_post(self, post_id: int) -> Optional[Post]:
        try:
            return self.db.query(Post).options(
                sqlalchemy.orm.joinedload(Post.user)
            ).filter(Post.id == post_id).first()
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching post: {str(e)}")

    def update_post(self, post_id: int, title: str = None, content: str = None,
                   visibility: str = None, type: str = None,
                   location: str = None, map_location: str = None,
                   price: float = None, status: str = None, is_anonymous: bool = None,
                   latitude: float = None, longitude: float = None) -> Optional[Post]:
        try:
            post = self.get_post(post_id)
            if post:
                if title is not None:
                    post.title = title
                if content is not None:
                    post.content = content
                if visibility is not None:
                    post.visibility = visibility
                if type is not None:
                    post.type = type
                if location is not None:
                    post.location = location
                # No map_location anymore
                if latitude is not None:
                    post.latitude = latitude
                if longitude is not None:
                    post.longitude = longitude
                if price is not None:
                    post.price = price
                if status is not None:
                    post.status = status
                if is_anonymous is not None:
                    post.is_anonymous = is_anonymous
                self.db.commit()
                self.db.refresh(post)
            return post
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while updating post: {str(e)}")

    def delete_post(self, post_id: int) -> bool:
        try:
            post = self.get_post(post_id)
            if post:
                self.db.delete(post)
                self.db.commit()
                return True
            return False
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while deleting post: {str(e)}")

    def get_posts_by_user(self, user_id: int, page: int = 1, limit: int = 10) -> Tuple[List[Post], int]:
        try:
            # Join with User table to get user information, similar to search_posts
            query = self.db.query(Post).join(User, Post.user_id == User.id).filter(Post.user_id == user_id)
            total = query.count()
            posts = query.order_by(desc(Post.created_at)).offset((page - 1) * limit).limit(limit).all()
            return posts, total
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching user posts: {str(e)}")

    def search_posts(self, type: str = None, location: str = None,
                    min_price: float = None, max_price: float = None,
                    status: str = None, page: int = 1, limit: int = 10) -> Tuple[List[Post], int]:
        try:
            print("Starting search_posts in repository")
            # Join with User table to get user information
            query = self.db.query(Post).join(User, Post.user_id == User.id)
            
            # Only apply filters if they are explicitly provided
            if type and type.strip():
                print(f"Filtering by type: {type}")
                query = query.filter(Post.type == type)
            if location and location.strip():
                print(f"Filtering by location: {location}")
                query = query.filter(Post.location.ilike(f"%{location}%"))
            if min_price is not None and min_price > 0:
                print(f"Filtering by min_price: {min_price}")
                query = query.filter(Post.price >= min_price)
            if max_price is not None and max_price > 0:
                print(f"Filtering by max_price: {max_price}")
                query = query.filter(Post.price <= max_price)
            if status and status.strip():
                print(f"Filtering by status: {status}")
                query = query.filter(Post.status == status)
            
            total = query.count()
            print(f"Total posts before pagination: {total}")

            # Calculate total pages
            total_pages = (total + limit - 1) // limit
            
            # Validate and adjust page number
            if total_pages == 0:
                total_pages = 1
            if page > total_pages:
                page = 1  # Reset to first page if requested page is beyond total pages
            
            # Calculate offset
            offset = (page - 1) * limit
            
            # Add options to eagerly load the user relationship
            query = query.options(
                sqlalchemy.orm.joinedload(Post.user)
            )
            
            posts = query.order_by(desc(Post.created_at)).offset(offset).limit(limit).all()
            print(f"Retrieved {len(posts)} posts after pagination")
            
            return posts, total
        except SQLAlchemyError as e:
            print(f"Database error in search_posts: {str(e)}")
            raise Exception(f"Database error while searching posts: {str(e)}")
        except Exception as e:
            print(f"Unexpected error in search_posts: {str(e)}")
            raise e

    # Media Operations
    def add_post_media(self, post_id: int, media_type: str, media_url: str,
                      media_order: int, media_size: int = 0, caption: str = None,
                      commit: bool = True) -> int:
        try:
            insert_stmt = MediaTable.insert().returning(MediaTable.c.id).values(
                context_id=post_id,
                context_type='post',
                media_type=media_type,
                media_url=media_url,
                media_order=media_order,
                media_size=media_size,
                caption=caption,
                uploaded_at=datetime.utcnow()
            )
            result = self.db.execute(insert_stmt)
            if commit:
                self.db.commit()
            return result.scalar()
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while adding media: {str(e)}")

    def delete_post_media(self, media_id: int) -> bool:
        try:
            delete_stmt = MediaTable.delete().where(MediaTable.c.id == media_id)
            result = self.db.execute(delete_stmt)
            self.db.commit()
            return result.rowcount > 0
        except SQLAlchemyError:
            self.db.rollback()
            return False

    def update_media_url_size(self, media_id: int, media_url: str, media_size: int, commit: bool = True) -> bool:
        try:
            upd = (
                update(MediaTable)
                .where(MediaTable.c.id == media_id)
                .values(media_url=media_url, media_size=media_size)
            )
            result = self.db.execute(upd)
            if commit:
                self.db.commit()
            return result.rowcount > 0
        except SQLAlchemyError:
            self.db.rollback()
            return False

    def get_post_media(self, post_id: int):
        try:
            stmt = (
                select(MediaTable)
                .where(and_(MediaTable.c.context_id == post_id, MediaTable.c.context_type == 'post'))
                .order_by(MediaTable.c.media_order)
            )
            result = self.db.execute(stmt).fetchall()
            return result
        except SQLAlchemyError as e:
            raise Exception(f"Database error while fetching media: {str(e)}")

    # Like Operations
    def like_post(self, post_id: int, user_id: int, reaction_type: str = 'like') -> Optional[Post]:
        try:
            # First check if post exists
            post = self.get_post(post_id)
            if not post:
                raise Exception(f"Post with ID {post_id} not found")

            # Check if user has already liked the post
            existing_like = self.db.query(PostLike).filter(
                PostLike.post_id == post_id,
                PostLike.user_id == user_id
            ).first()
            
            if not existing_like:
                try:
                    like = PostLike(
                        post_id=post_id,
                        user_id=user_id,
                        reaction_type=reaction_type,
                        liked_at=datetime.utcnow()
                    )
                    self.db.add(like)
                    self.db.commit()
                except IntegrityError:
                    # Unique index collision (duplicate like): treat as idempotent success
                    self.db.rollback()
                except SQLAlchemyError as e:
                    self.db.rollback()
                    raise Exception(f"Database error while adding like: {str(e)}")
            
            # Get fresh post data with updated like count
            try:
                self.db.refresh(post)
                return post
            except SQLAlchemyError as e:
                self.db.rollback()
                raise Exception(f"Database error while refreshing post: {str(e)}")
        except Exception as e:
            self.db.rollback()
            raise e

    def unlike_post(self, post_id: int, user_id: int) -> Optional[Post]:
        try:
            # First check if post exists
            post = self.get_post(post_id)
            if not post:
                raise Exception(f"Post with ID {post_id} not found")

            # Check if user has liked the post
            like = self.db.query(PostLike).filter(
                PostLike.post_id == post_id,
                PostLike.user_id == user_id
            ).first()
            
            if like:
                try:
                    self.db.delete(like)
                    self.db.commit()
                except SQLAlchemyError as e:
                    self.db.rollback()
                    raise Exception(f"Database error while removing like: {str(e)}")
            
            # Get fresh post data with updated like count
            try:
                self.db.refresh(post)
                return post
            except SQLAlchemyError as e:
                self.db.rollback()
                raise Exception(f"Database error while refreshing post: {str(e)}")
        except Exception as e:
            self.db.rollback()
            raise e

    # Comment Operations
    def create_comment(self, post_id: int, user_id: int, comment_text: str,
                      parent_comment_id: int = None) -> Comment:
        try:
            # For replies (parent_comment_id > 0), verify parent comment exists
            if parent_comment_id and parent_comment_id > 0:
                parent_comment = self.get_comment(parent_comment_id)
                if not parent_comment:
                    raise Exception(f"Parent comment with ID {parent_comment_id} not found")
                # Verify parent comment belongs to the same post
                if parent_comment.post_id != post_id:
                    raise Exception("Parent comment does not belong to the specified post")
            else:
                # For new comments (parent_comment_id = 0 or None), set to None
                parent_comment_id = None

            # Create the comment
            comment = Comment(
                post_id=post_id,
                user_id=user_id,
                comment=comment_text,
                parent_comment_id=parent_comment_id,  # Will be NULL for new comments, actual ID for replies
                added_at=datetime.utcnow(),
                commented_at=datetime.utcnow(),
                status='active'
            )
            self.db.add(comment)
            self.db.commit()
            self.db.refresh(comment)
            return comment
        except SQLAlchemyError as e:
            self.db.rollback()
            raise Exception(f"Database error while creating comment: {str(e)}")
        except Exception as e:
            self.db.rollback()
            raise e

    def get_comment(self, comment_id: int) -> Optional[Comment]:
        return self.db.query(Comment).filter(Comment.id == comment_id).first()

    def get_comment_replies(self, comment_id: int, page: int = 1, limit: int = 10) -> Tuple[List[Comment], int]:
        query = self.db.query(Comment).filter(
            Comment.parent_comment_id == comment_id
        )
        total = query.count()
        replies = query.order_by(desc(Comment.commented_at)).offset((page - 1) * limit).limit(limit).all()
        return replies, total

    def update_comment(self, comment_id: int, comment_text: str = None,
                      status: str = None) -> Optional[Comment]:
        comment = self.get_comment(comment_id)
        if comment:
            if comment_text is not None:
                comment.comment = comment_text
                comment.commented_at = datetime.utcnow()
            if status is not None:
                comment.status = status
            self.db.commit()
            self.db.refresh(comment)
        return comment

    def delete_comment(self, comment_id: int) -> bool:
        comment = self.get_comment(comment_id)
        if comment:
            self.db.delete(comment)
            self.db.commit()
            return True
        return False

    def get_comment_thread(self, comment_id: int) -> List[Comment]:
        """Get a comment and all its nested replies in a flat list"""
        def get_replies(comment):
            result = [comment]
            for reply in comment.replies:
                result.extend(get_replies(reply))
            return result

        comment = self.get_comment(comment_id)
        if not comment:
            return []
        
        return get_replies(comment)

    def get_comments(self, post_id: int, page: int = 1, limit: int = 10) -> Tuple[List[Comment], int]:
        try:
            print(f"get_comments called with post_id: {post_id}, page: {page}, limit: {limit}")
            
            # Get only top-level comments (no parent)
            query = self.db.query(Comment).options(
                sqlalchemy.orm.joinedload(Comment.user)
            ).filter(
                Comment.post_id == post_id,
                Comment.parent_comment_id.is_(None)
            ).order_by(desc(Comment.commented_at))

            # Print the SQL query
            print(f"SQL Query: {query}")
            
            # Get total count before pagination
            total = query.count()
            print(f"Total comments found: {total}")

            # Apply pagination
            offset = (page - 1) * limit
            print(f"Using offset: {offset}, limit: {limit}")
            
            comments = query.offset(offset).limit(limit).all()
            print(f"Retrieved {len(comments)} comments")
            
            # Debug print each comment
            for comment in comments:
                print(f"Comment ID: {comment.id}, User ID: {comment.user_id}, "
                      f"User: {comment.user.first_name if comment.user else 'None'} "
                      f"{comment.user.last_name if comment.user else 'None'}, "
                      f"Role: {comment.user.role if comment.user else 'None'}")
                print(f"Has {len(comment.replies)} replies")

            return comments, total
        except SQLAlchemyError as e:
            print(f"Database error in get_comments: {str(e)}")
            raise Exception(f"Database error while getting comments: {str(e)}")
        except Exception as e:
            print(f"Unexpected error in get_comments: {str(e)}")
            raise e

    def like_comment(self, comment_id: int, user_id: int, reaction_type: str = 'like') -> Optional[Comment]:
        try:
            # First check if comment exists
            comment = self.get_comment(comment_id)
            if not comment:
                raise Exception(f"Comment with ID {comment_id} not found")

            # Check if user has already liked the comment
            existing_like = self.db.query(CommentLike).filter(
                CommentLike.comment_id == comment_id,
                CommentLike.user_id == user_id
            ).first()
            
            if not existing_like:
                try:
                    like = CommentLike(
                        comment_id=comment_id,
                        user_id=user_id,
                        reaction_type=reaction_type,
                        liked_at=datetime.utcnow()
                    )
                    self.db.add(like)
                    self.db.commit()
                except IntegrityError:
                    # Duplicate like: idempotent success
                    self.db.rollback()
                except SQLAlchemyError as e:
                    self.db.rollback()
                    raise Exception(f"Database error while adding like: {str(e)}")
            
            # Get fresh comment data with updated like count
            try:
                self.db.refresh(comment)
                return comment
            except SQLAlchemyError as e:
                self.db.rollback()
                raise Exception(f"Database error while refreshing comment: {str(e)}")
        except Exception as e:
            self.db.rollback()
            raise e

    def unlike_comment(self, comment_id: int, user_id: int) -> Optional[Comment]:
        try:
            # First check if comment exists
            comment = self.get_comment(comment_id)
            if not comment:
                raise Exception(f"Comment with ID {comment_id} not found")

            # Check if user has liked the comment
            like = self.db.query(CommentLike).filter(
                CommentLike.comment_id == comment_id,
                CommentLike.user_id == user_id
            ).first()
            
            if like:
                try:
                    self.db.delete(like)
                    self.db.commit()
                except SQLAlchemyError as e:
                    self.db.rollback()
                    raise Exception(f"Database error while removing like: {str(e)}")
            
            # Get fresh comment data with updated like count
            try:
                self.db.refresh(comment)
                return comment
            except SQLAlchemyError as e:
                self.db.rollback()
                raise Exception(f"Database error while refreshing comment: {str(e)}")
        except Exception as e:
            self.db.rollback()
            raise e

    # Helper Methods
    def get_post_like_count(self, post_id: int) -> int:
        return self.db.query(PostLike).filter(PostLike.post_id == post_id).count()

    def get_comment_like_count(self, comment_id: int) -> int:
        return self.db.query(CommentLike).filter(CommentLike.comment_id == comment_id).count()

    def get_post_comment_count(self, post_id: int) -> int:
        # Count only top-level comments
        return self.db.query(Comment).filter(
            Comment.post_id == post_id,
            Comment.parent_comment_id.is_(None)
        ).count() 
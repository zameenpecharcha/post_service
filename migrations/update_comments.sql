-- Drop the foreign key constraint
ALTER TABLE comments DROP CONSTRAINT IF EXISTS comments_parent_comment_id_fkey;

-- Make parent_comment_id nullable without default
ALTER TABLE comments 
    ALTER COLUMN parent_comment_id DROP DEFAULT,
    ALTER COLUMN parent_comment_id DROP NOT NULL;

-- Add back the foreign key constraint
ALTER TABLE comments 
    ADD CONSTRAINT comments_parent_comment_id_fkey 
    FOREIGN KEY (parent_comment_id) 
    REFERENCES comments(id) 
    ON DELETE CASCADE; 
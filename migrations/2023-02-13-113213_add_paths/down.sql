-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS paths;
DROP INDEX uniqe_collection_name_project_id;
DROP INDEX unique_project_name;

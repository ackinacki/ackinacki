DROP TABLE block_markers;
DROP TABLE candidate_blocks;
CREATE TABLE candidate_blocks (id INTEGER PRIMARY KEY, oid TEXT NOT NULL UNIQUE, data BLOB);

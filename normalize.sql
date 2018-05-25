CREATE TABLE place(id INTEGER PRIMARY KEY AUTOINCREMENT,
                   location TEXT);

CREATE TABLE user(id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                  lang TEXT,
                  fk_place REFERENCES place(id) ON DELETE CASCADE);

ALTER TABLE tweet DROP COLUMN userid;

ALTER TABLE tweet ADD CONSTRAINT fk_user
                  FOREIGN KEY (id)
                  REFERENCES user(id);

ALTER TABLE tweet ADD COLUMN userid INTEGER REFERENCES parent(id);


-- WAS
-- tweet
-----------------
-- id
-- name
-- tweet_text
-- country_code
-- display_url
-- lang
-- created_at
-- location
-- tweet_sentiment

-- TO BE

-- user
-----------------
-- id
-- name
-- lang
-- placeid

-- place
-------------------
-- id
-- location

-- tweet
-----------------
-- id
-- name
-- userid
-- country_code
-- display_url
-- created_at
-- tweet_sentiment


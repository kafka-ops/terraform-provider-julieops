-- table with declared columns:
CREATE TABLE users (
     id BIGINT PRIMARY KEY,
     gender VARCHAR,
     region_id BIGINT,
     profile_id BIGINT
   ) WITH (
     KAFKA_TOPIC = 'users',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);



CREATE TABLE region (
     id BIGINT PRIMARY KEY,
     name VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'regions',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);



CREATE TABLE profile (
     id BIGINT PRIMARY KEY,
     name VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'profiles',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);

INSERT INTO users (id, gender, region_id, profile_id)  VALUES (1, 'M', 1, 1 );
INSERT INTO users (id, gender, region_id, profile_id)  VALUES (2, 'M', 2, 2 );
INSERT INTO users (id, gender, region_id, profile_id)  VALUES (3, 'F', 3, 1 );
INSERT INTO users (id, gender, region_id, profile_id)  VALUES (4, 'F', 1, 2 );
INSERT INTO users (id, gender, region_id, profile_id)  VALUES (5, 'M', 2, 1 );

INSERT INTO region (id, name)  VALUES (1, 'Germany' );
INSERT INTO region (id, name)  VALUES (2, 'Spain' );
INSERT INTO region (id, name)  VALUES (3, 'UK' );
INSERT INTO region (id, name)  VALUES (4, 'Spain' );
INSERT INTO region (id, name)  VALUES (5, 'UK' );

INSERT INTO profile (id, name)  VALUES (1, 'H' );
INSERT INTO profile (id, name)  VALUES (2, 'L' );
INSERT INTO profile (id, name)  VALUES (3, 'H' );
INSERT INTO profile (id, name)  VALUES (4, 'L' );
INSERT INTO profile (id, name)  VALUES (5, 'L' );

INSERT INTO region (id, name)  VALUES (4, 'France' );
INSERT INTO profile (id, name)  VALUES (3, 'M' );


CREATE TABLE global AS
  SELECT *
  FROM users u INNER JOIN region r ON u.id = r.id
               INNER JOIN profile p ON u.id = p.id
  EMIT CHANGES;

CREATE TABLE uglobal AS
    SELECT UDF_NORM(u.gender), r.name
    FROM users u INNER JOIN region r ON u.id = r.id
                 INNER JOIN profile p ON u.id = p.id
    EMIT CHANGES;

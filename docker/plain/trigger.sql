---
CREATE TABLE Users (
     id BIGINT PRIMARY KEY,
     gender VARCHAR,
     region_id BIGINT,
     profile_id BIGINT,
     action VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'users',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);

CREATE TABLE Region (
     id BIGINT PRIMARY KEY,
     name VARCHAR,
     action VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'regions',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);


CREATE TABLE Profile (
     id BIGINT PRIMARY KEY,
     name VARCHAR,
     action VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'profiles',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);

CREATE TABLE Trigger (
     action VARCHAR PRIMARY KEY,
     running BOOLEAN
   ) WITH (
     KAFKA_TOPIC = 'triggers',
     PARTITIONS = 1,
     REPLICAS = 1,
     VALUE_FORMAT = 'JSON'
);

INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (1, 'M', 1, 1, 'trigger' );
INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (2, 'M', 2, 2, 'trigger' );
INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (3, 'F', 3, 1, 'trigger' );
INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (4, 'F', 1, 2, 'trigger' );
INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (5, 'M', 2, 1, 'trigger' );

INSERT INTO Region (id, name, action)  VALUES (1, 'Germany', 'trigger' );
INSERT INTO Region (id, name, action)  VALUES (2, 'Spain', 'trigger' );
INSERT INTO Region (id, name, action)  VALUES (3, 'UK', 'trigger' );
INSERT INTO Region (id, name, action)  VALUES (4, 'Spain', 'trigger' );
INSERT INTO Region (id, name, action)  VALUES (5, 'UK', 'trigger' );

INSERT INTO Profile (id, name, action)  VALUES (1, 'H', 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (2, 'L', 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (3, 'H', 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (4, 'L', 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (5, 'L', 'trigger' );

INSERT INTO Region (id, name, action)  VALUES (4, 'France', 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (3, 'M', 'trigger' );


INSERT INTO Trigger  VALUES ('trigger', false );

CREATE TABLE global AS
SELECT u.id, u.gender, p.name as profile, r.name as region
FROM users u INNER JOIN Trigger t ON t.action = u.action
             INNER JOIN Profile p ON u.id = p.id
             INNER JOIN Region r ON u.id = r.id
WHERE t.running = true
EMIT CHANGES;

INSERT INTO Trigger  VALUES ('trigger', true );


INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (6, 'F', 2, 1, 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (6, 'XL', 'trigger' );
INSERT INTO Region (id, name, action)  VALUES (6, 'Catalonia', 'trigger' );

INSERT INTO Users (id, gender, region_id, profile_id, action)  VALUES (7, 'M', 2, 1, 'trigger' );
INSERT INTO Profile (id, name, action)  VALUES (7, 'XS', 'trigger' );
INSERT INTO Region (id, name, action)  VALUES (7, 'Catalonia', 'trigger' );

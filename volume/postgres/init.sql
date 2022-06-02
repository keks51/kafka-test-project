CREATE TABLE offsets (
	load_id serial,
	app_name VARCHAR ( 50 ) NOT NULL,
	topic VARCHAR ( 50 ) NOT NULL,
	start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    start_offset bigint NOT NULL,
    end_offset bigint NOT NULL);

CREATE TABLE users (
	user_id serial,
	name VARCHAR ( 50 ) NOT NULL,
	age integer NOT NULL);

INSERT INTO users(name, age)
VALUES
('James', 25),
('Robert', 43),
('John', 65),
('Michael', 32),
('David', 56),
('William', 37),
('Richard', 36),
('Joseph', 74),
('Thomas', 61),
('Charles', 42);
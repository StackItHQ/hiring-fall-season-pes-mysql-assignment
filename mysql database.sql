USE Project;  -- Select the database

CREATE TABLE details (
  id INT PRIMARY KEY AUTO_INCREMENT,               -- Primary key with auto-increment
  name VARCHAR(255) NOT NULL,                      -- Name cannot be NULL
  age INT CHECK (age > 0),                         -- Age must be positive
  email VARCHAR(255) UNIQUE                       -- Email must be unique
);

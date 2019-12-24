CREATE TABLE log
(
  sequence INTEGER PRIMARY KEY
  value    BLOB
);

CREATE TABLE data
(
  last_log_sequence INTEGER
  value    BLOB
);

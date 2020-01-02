CREATE TABLE raftlog (
  log_index   INTEGER PRIMARY KEY,
  term        INTEGER,
  command     BLOB
);

CREATE TABLE raftlog (
  log_index   INTEGER PRIMARY KEY,
  term_number INTEGER,
  command     BLOB
);

CREATE TABLE raftlog (
  log_index   INTEGER PRIMARY KEY AUTOINCREMENT,
  term_number INTEGER,
  command     BLOB
);

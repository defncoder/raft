CREATE TABLE raftlog (
  idx         INTEGER PRIMARY KEY,
  term        INTEGER,
  command     BLOB
);

CREATE TABLE raftlog (
  idx         INTEGER,
  term        INTEGER,
  command     BLOB,
  PRIMARY KEY(idx, term)
);

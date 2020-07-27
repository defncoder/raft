CREATE TABLE raftlog (
  idx         INTEGER NOT NULL,
  term        INTEGER NOT NULL,
  requestid   TEXT,
  command     TEXT,
  PRIMARY KEY(idx, term)
);

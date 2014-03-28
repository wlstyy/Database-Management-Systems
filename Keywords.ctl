LOAD DATA
INFILE 'Keywords.dat'
APPEND
INTO TABLE Keywords
FIELDS TERMINATED BY X'9'
(advertiserId, keyword, bid)


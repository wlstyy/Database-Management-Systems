LOAD DATA
INFILE 'Advertisers.dat'
APPEND
INTO TABLE Advertisers
FIELDS TERMINATED BY X'9'
(advertiserId,budget,ctc)


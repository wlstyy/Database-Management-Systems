set termout off
CREATE TYPE Tokens AS TABLE OF VARCHAR(1000);
/

CREATE TYPE INFO AS OBJECT(
		qid	INTEGER,
		advertiserId	INTEGER,
		ad_rank	FLOAT,
		balance	FLOAT,
		budget	FLOAT,
		total_bid	FLOAT,
		impressions	INTEGER,
		ctc	FLOAT
		);
/

CREATE TYPE VFLOAT AS TABLE OF FLOAT;
/

CREATE TYPE VINFO AS TABLE OF INFO;
/

CREATE FUNCTION split(input IN CHAR)
	RETURN Tokens
	IS substrings	Tokens;
	BEGIN
		SELECT CAST(REGEXP_SUBSTR(input, '[^ ]+', 1, level) AS VARCHAR(1000)) BULK COLLECT INTO substrings
		FROM DUAL
		CONNECT BY REGEXP_SUBSTR(input, '[^ ]+', 1, level) IS NOT NULL;
		RETURN(substrings);
	END;
/

CREATE TABLE Ads(
	advertiserId	INTEGER,
	keywordSet	Tokens,
	budget	FLOAT,
	balance	FLOAT,
	impressions	INTEGER,
	ctc	FLOAT
)NESTED TABLE keywordSet STORE AS keys;

CREATE TABLE CACHE(
	qid	INTEGER,
	advertiserId	INTEGER,
	total_bid	FLOAT,
	quality	FLOAT
);

CREATE FUNCTION getQuality(querySet IN Tokens, keySet IN Tokens, ctc IN FLOAT)
	RETURN FLOAT
	IS	quality	FLOAT;
	keyCount	FLOAT;
	queryCount	FLOAT;
	keyVector	VFLOAT:=VFLOAT();
	queryVector	VFLOAT:=VFLOAT();
	matchedWords	Tokens;
	top	FLOAT;
	qSum	FLOAT;
	kSum	FLOAT;
	i	INTEGER;
	j	INTEGER;
	BEGIN
	SELECT * BULK COLLECT INTO matchedWords
	FROM(
		SELECT * FROM TABLE(querySet)
		UNION
		SELECT * FROM TABLE(keySet)
		);
	keyVector.EXTEND(matchedWords.COUNT);
	queryVector.EXTEND(matchedWords.COUNT);
	i:=matchedWords.FIRST;
	WHILE i IS NOT NULL LOOP
		keyCount:=0;
		j:=keySet.FIRST;
		WHILE j IS NOT NULL LOOP
			IF keySet(j) LIKE matchedWords(i) THEN
				keyCount:=keyCount+1;
			END IF;
			j:=keySet.NEXT(j);
		END LOOP;
		queryCount:=0;
		j:=querySet.FIRST;
		WHILE j IS NOT NULL LOOP
			IF querySet(j) LIKE matchedWords(i) THEN
				queryCount:=queryCount+1;
			END IF;
			j:=querySet.NEXT(j);
		END LOOP;
		keyVector(i):=keyCount;
		queryVector(i):=queryCount;
		i:=matchedWords.NEXT(i);
	END LOOP;
	qSum:=0;
	kSum:=0;
	top:=0;
	i:=keyVector.FIRST;
	WHILE i IS NOT NULL LOOP
		top:=top+keyVector(i)*queryVector(i);
		qSum:=qSum+queryVector(i)*queryVector(i);
		kSum:=kSum+keyVector(i)*keyVector(i);
		i:=keyVector.NEXT(i);
	END LOOP;
	quality:=top/(SQRT(qSum)*SQRT(kSum))*ctc;
	RETURN(quality);
	END;
/	

DECLARE
  substrs Tokens;
  CURSOR C1 IS
  SELECT qid, query FROM QUERIES
  order by qid;
  temp VINFO;
  q C1%ROWTYPE;
  K integer:= &1;
  BEGIN

  INSERT INTO ADS
  (advertiserId, keywordSet, budget, balance, impressions, ctc)
  SELECT advertiserId, keywordSet, budget, budget, 0, ctc
  FROM (SELECT advertiserId, CAST(COLLECT(keyword) AS Tokens) as keywordSet FROM Keywords GROUP BY advertiserId)
  JOIN Advertisers USING (advertiserId);


  OPEN C1;
  LOOP
  FETCH C1 INTO q;
  EXIT WHEN C1%NOTFOUND;
  substrs:=split(q.query);

INSERT INTO CACHE
(qid, advertiserId, total_bid, quality)
  SELECT q.qid, advertiserId, total_bid, getQuality(substrs, keywordSet, ctc)
FROM
( 
SELECT advertiserID, SUM(bid) AS total_bid
    FROM Keywords
    WHERE keyword MEMBER OF substrs
    GROUP BY advertiserId
) JOIN ADS USING (advertiserId);

SELECT INFO(q.qid, advertiserId, ad_rank,balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO temp
FROM (SELECT advertiserId, ad_rank, balance, budget, total_bid, impressions, ctc
	FROM (SELECT advertiserId, quality*total_bid AS ad_rank, total_bid FROM CACHE WHERE qid=q.qid) JOIN Ads USING (advertiserId)
	WHERE balance>=total_bid
	ORDER BY ad_rank DESC, advertiserId)
WHERE ROWNUM<=K;

IF temp.COUNT>0 THEN
  FOR i in temp.FIRST..temp.LAST
 LOOP
 IF MOD(temp(i).impressions,100)<temp(i).ctc*100 THEN
 temp(i).balance:=temp(i).balance-temp(i).total_bid;
 END IF;
 END LOOP;
 END IF;

  INSERT INTO TASK1
	(qid, rank, advertiserId, balance, budget)
	SELECT q.qid, ROWNUM, advertiserId, balance, budget
	FROM TABLE(temp);
	COMMIT;
FORALL i IN temp.FIRST..temp.LAST
 	UPDATE Ads
 	SET balance=temp(i).balance,
 	    impressions=impressions+1
 	WHERE advertiserId=temp(i).advertiserId;
COMMIT;
  END LOOP;
  CLOSE C1;
  
  UPDATE ADS SET impressions=0,balance=budget;
  COMMIT;
END;
/

DECLARE
CURSOR C4 IS
SELECT qid, query FROM QUERIES
ORDER BY qid;
tmp VINFO;
temp VINFO;
q C4%ROWTYPE;
nextbid FLOAT;
K INTEGER:= &2; --&2;
BEGIN
OPEN C4;
LOOP
FETCH C4 INTO q;
EXIT WHEN C4%NOTFOUND;

SELECT INFO(q.qid, advertiserId, quality*total_bid, balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO tmp
FROM (SELECT advertiserId, quality, total_bid FROM CACHE WHERE qid=q.qid) JOIN Ads USING (advertiserId)
WHERE balance>=total_bid;

SELECT INFO(q.qid, advertiserId, ad_rank,balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO temp
FROM(SELECT * FROM TABLE(tmp) ORDER BY ad_rank DESC, advertiserId)
WHERE ROWNUM<=K;

IF temp.COUNT>0 THEN
  FOR i in temp.FIRST..temp.LAST
 LOOP
 IF MOD(temp(i).impressions,100)<temp(i).ctc*100 THEN
 SELECT (CASE WHEN MAX(total_bid) IS NULL THEN temp(i).total_bid ELSE MAX(total_bid) END)
 into nextbid
 FROM TABLE(tmp)
 WHERE total_bid<temp(i).total_bid;
 temp(i).balance:=temp(i).balance-nextbid;
 END IF;
 END LOOP;
 END IF;
INSERT INTO TASK2
  (qid, rank, advertiserId, balance, budget)
  SELECT q.qid, ROWNUM, advertiserId, balance, budget
  FROM TABLE(temp);
COMMIT;
FORALL i IN temp.FIRST..temp.LAST
  UPDATE Ads
  SET balance=temp(i).balance,
      impressions=impressions+1
  WHERE advertiserId=temp(i).advertiserId;
COMMIT;
END LOOP;
  CLOSE C4;
  UPDATE Ads SET impressions=0, balance=budget;
  COMMIT; 
END;
/

DECLARE
CURSOR C2 IS
SELECT qid, query FROM QUERIES order by qid;
temp VINFO;
q C2%ROWTYPE;
K Integer:= &3; --&3;
BEGIN
OPEN C2;
LOOP
FETCH C2 INTO q;
EXIT WHEN C2%NOTFOUND;
SELECT INFO(q.qid, advertiserId, ad_rank,balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO temp
FROM (SELECT advertiserId, quality*balance AS ad_rank, balance, budget, total_bid, impressions, ctc
  FROM (SELECT advertiserId, quality, total_bid FROM CACHE WHERE qid=q.qid) JOIN Ads USING (advertiserId)
  WHERE balance>=total_bid
  ORDER BY ad_rank DESC, advertiserId)
WHERE ROWNUM<=K;
IF temp.COUNT>0 THEN
  FOR i in temp.FIRST..temp.LAST
 LOOP
 IF MOD(temp(i).impressions,100)<temp(i).ctc*100 THEN
 temp(i).balance:=temp(i).balance-temp(i).total_bid;
 END IF;
 END LOOP;
 END IF;
INSERT INTO TASK3
  (qid, rank, advertiserId, balance, budget)
  SELECT q.qid, ROWNUM, advertiserId, balance, budget
  FROM TABLE(temp);
COMMIT;
FORALL i IN temp.FIRST..temp.LAST
  UPDATE Ads
  SET balance=temp(i).balance,
      impressions=impressions+1
  WHERE advertiserId=temp(i).advertiserId;
COMMIT;
END LOOP;
  CLOSE C2;
  UPDATE Ads SET impressions=0, balance=budget;
  COMMIT; 
END;
/

DECLARE
CURSOR C5 IS
SELECT qid, query FROM QUERIES
ORDER BY qid;
tmp VINFO;
temp VINFO;
q C5%ROWTYPE;
nextbid FLOAT;
K INTEGER:= &4; --&4;
BEGIN
OPEN C5;
LOOP
FETCH C5 INTO q;
EXIT WHEN C5%NOTFOUND;
SELECT INFO(q.qid, advertiserId, quality*balance, balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO tmp
FROM (SELECT advertiserId, quality, total_bid FROM CACHE WHERE qid=q.qid) JOIN Ads USING (advertiserId)
WHERE balance>=total_bid;

SELECT INFO(q.qid, advertiserId, ad_rank,balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO temp
FROM(SELECT * FROM TABLE(tmp) ORDER BY ad_rank DESC, advertiserId)
WHERE ROWNUM<=K;

IF temp.COUNT>0 THEN
  FOR i in temp.FIRST..temp.LAST
 LOOP
 IF MOD(temp(i).impressions,100)<temp(i).ctc*100 THEN
 SELECT (CASE WHEN MAX(total_bid) IS NULL THEN temp(i).total_bid ELSE MAX(total_bid) END)
 into nextbid
 FROM TABLE(tmp)
 WHERE total_bid<temp(i).total_bid;
 temp(i).balance:=temp(i).balance-nextbid;
 END IF;
 END LOOP;
 END IF;
INSERT INTO TASK4
  (qid, rank, advertiserId, balance, budget)
  SELECT q.qid, ROWNUM, advertiserId, balance, budget
  FROM TABLE(temp);
COMMIT;
FORALL i IN temp.FIRST..temp.LAST
  UPDATE Ads
  SET balance=temp(i).balance,
      impressions=impressions+1
  WHERE advertiserId=temp(i).advertiserId;
COMMIT;
END LOOP;
  CLOSE C5;
  UPDATE Ads SET impressions=0, balance=budget;
  COMMIT; 
END;
/

DECLARE
CURSOR C3 IS
SELECT qid, query FROM QUERIES order by qid;
temp VINFO;
q C3%ROWTYPE;
K INTEGER:= &5; --&5;
BEGIN
OPEN C3;
LOOP
FETCH C3 INTO q;
EXIT WHEN C3%NOTFOUND;
SELECT INFO(q.qid, advertiserId, ad_rank,balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO temp
FROM (SELECT advertiserId, quality*total_bid*(1-EXP(-1*balance/budget)) AS ad_rank, balance, budget, total_bid, impressions, ctc
  FROM (SELECT advertiserId, quality, total_bid FROM CACHE WHERE qid=q.qid) JOIN Ads USING (advertiserId)
  WHERE balance>=total_bid
  ORDER BY ad_rank DESC, advertiserId)
WHERE ROWNUM<=K;
IF temp.COUNT>0 THEN
  FOR i in temp.FIRST..temp.LAST
 LOOP
 IF MOD(temp(i).impressions,100)<temp(i).ctc*100 THEN
 temp(i).balance:=temp(i).balance-temp(i).total_bid;
 END IF;
 END LOOP;
 END IF;
INSERT INTO TASK5
  (qid, rank, advertiserId, balance, budget)
  SELECT q.qid, ROWNUM, advertiserId, balance, budget
  FROM TABLE(temp);
COMMIT;
FORALL i IN temp.FIRST..temp.LAST
  UPDATE Ads
  SET balance=temp(i).balance,
      impressions=impressions+1
  WHERE advertiserId=temp(i).advertiserId;
COMMIT;
END LOOP;
  CLOSE C3;
  UPDATE ADS SET impressions=0,balance=budget;
  COMMIT;
END;
/

DECLARE
CURSOR C6 IS
SELECT qid, query FROM QUERIES
ORDER BY qid;
temp VINFO;
tmp VINFO;
q C6%ROWTYPE;
nextbid FLOAT;
K INTEGER:= &6; --&6;
BEGIN
OPEN C6;
LOOP
FETCH C6 INTO q;
EXIT WHEN C6%NOTFOUND;

SELECT INFO(q.qid, advertiserId, quality*total_bid*(1-EXP(-1*balance/budget)), balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO tmp
FROM (SELECT advertiserId, quality, total_bid FROM CACHE WHERE qid=q.qid) JOIN Ads USING (advertiserId)
WHERE balance>=total_bid;

SELECT INFO(q.qid, advertiserId, ad_rank,balance, budget, total_bid, impressions, ctc)
BULK COLLECT INTO temp
FROM(SELECT * FROM TABLE(tmp) ORDER BY ad_rank DESC, advertiserId)
WHERE ROWNUM<=K;
IF temp.COUNT>0 THEN
  FOR i in temp.FIRST..temp.LAST
 LOOP
 IF MOD(temp(i).impressions,100)<temp(i).ctc*100 THEN
 SELECT (CASE WHEN MAX(total_bid) IS NULL THEN temp(i).total_bid ELSE MAX(total_bid) END)
 into nextbid
 FROM TABLE(tmp)
 WHERE total_bid<temp(i).total_bid;
 temp(i).balance:=temp(i).balance-nextbid;
 END IF;
 END LOOP;
 END IF;
INSERT INTO TASK6
  (qid, rank, advertiserId, balance, budget)
  SELECT q.qid, ROWNUM, advertiserId, balance, budget
  FROM TABLE(temp);
COMMIT;
FORALL i IN temp.FIRST..temp.LAST
  UPDATE Ads
  SET balance=temp(i).balance,
      impressions=impressions+1
  WHERE advertiserId=temp(i).advertiserId;
COMMIT;
END LOOP;
  CLOSE C6;
END;
/

set echo off

set feedback off

set linesize 9999

set heading off

set pagesize 0

set sqlprompt ''

set trimspool on

spool system.out.1


select qid||', '||rank||', '||advertiserid||', '||balance||', '||budget
from TASK1
order by qid, rank;

spool off

spool system.out.2

select qid||', '||rank||', '||advertiserid||', '||balance||', '||budget
from TASK2
order by qid, rank;

spool off

spool system.out.3

select qid||', '||rank||', '||advertiserid||', '||balance||', '||budget
from TASK3
order by qid, rank;

spool off

spool system.out.4

select qid||', '||rank||', '||advertiserid||', '||balance||', '||budget
from TASK4
order by qid, rank;

spool off

spool system.out.5

select qid||', '||rank||', '||advertiserid||', '||balance||', '||budget
from TASK5
order by qid, rank;

spool off

spool system.out.6

select qid||', '||rank||', '||advertiserid||', '||balance||', '||budget
from TASK6
order by qid, rank;

spool off
exit;

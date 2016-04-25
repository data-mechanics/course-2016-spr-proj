CREATE EXTERNAL TABLE IF NOT EXISTS cld (cid int, lid int, dist string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION 's3://th8-cs591-mapr/cld/';   

CREATE TABLE IF NOT EXISTS clda (cid int, lid int, dist double);
INSERT OVER TABLE clda SELECT cid, lid, DOUBLE(SPLIT(DIST, ' ')[0]) FROM cld;

CREATE TABLE IF NOT EXISTS dist_count (dist int, count int);
INSERT OVERWRITE TABLE dist_count SELECT dist, COUNT(dist) FROM (SELECT INT(dist) AS dist FROM clda) t0 GROUP BY dist;

SELECT CORR(dist, count) FROM dist_count WHERE dist < 5000;
SELECT STDDEV(dist, count) FROM dist_count WHERE dist < 5000;

CREATE EXTERNAL TABLE s3_dc (dist int, count int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION 's3://th8-cs591-mapr/output/';

INSERT OVERWRITE TABLE s3_dc SELECT * FROM dist_count WHERE dist < 5000;

CREATE EXTERNAL TABLE s3_table (cid int, clat double, clong double, time string, type string, lid int, llat double, llong double, dist double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION 's3://th8-cs591-mapr/outputtable/';

INSERT OVERWRITE TABLE s3_table 
SELECT  cid, clat, clong, time, type, lights.sid, lights.lat, lights.long, dist
FROM (SELECT c.sid AS cid, c.lat as clat, c.long AS clong, c.time AS time, c.type AS type, cld.lid AS lid, cld.dist AS dist FROM crimes AS c JOIN clda AS cld ON c.sid = cld.cid WHERE cld.dist < 5000) t JOIN lights ON t.lid = lights.sid;
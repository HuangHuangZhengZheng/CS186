-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
 SELECT MAX(era)
 FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT  namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
  ORDER BY namefirst ASC, namelast ASC -- 双排序！
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear ASC
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  HAVING AVG(height) > 70
  ORDER BY birthyear ASC-- 筛选的是group！
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, P.playerID AS playerid, yearid
  FROM people AS P, HallofFame AS H
  WHERE H.inducted = 'Y' AND P.playerID = H.playerID -- 注意join，否则deadlock or infinite loop！
  ORDER BY yearid DESC, playerid ASC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, P.playerID, C.schoolID, yearid
  FROM people AS P, CollegePlaying AS C, HallofFame AS H, Schools AS S
  WHERE P.playerID = C.playerID AND C.playerID = H.playerID AND C.schoolID = S.schoolID
        AND S.schoolState = 'CA' AND H.inducted = 'Y' -- 好抽象的多表链接
  ORDER BY yearid DESC, C.schoolID ASC, P.playerID ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT p.playerID, namefirst, namelast, c.schoolID
  FROM  people as p
  left outer join CollegePlaying as c
  on p.playerID = c.playerID

  inner join HallofFame as h
  on p.playerID = h.playerID
  -- people is "primary"
  WHERE h.inducted = 'Y'
  order by p.playerID desc, c.schoolID asc
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerID, namefirst, namelast, yearid, (b.H+b.H2B+2.0*b.H3B+3.0*b.HR)/(b.AB*1.0) as slg
  FROM people as p inner join batting as b
  on p.playerID = b.playerID
  where AB > 50
  order by slg desc, yearid asc, p.playerID asc
  limit 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerID, namefirst, namelast, ( SUM(b.H) + 1.0 * SUM(b.H2B) + 2.0 * SUM(b.H3B)
                                                     + 3.0 * SUM(b.HR)) / sum(b.AB) as lslg
                                                     -- 聚类函数算术表达式
  from people as p inner join batting as b on p.playerID = b.playerID
  group by p.playerID, namefirst, namelast -- 多聚类
  having sum(b.AB) > 50
  order by lslg desc, p.playerID asc
  limit 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT namefirst, namelast, (SUM(b.H) + 1.0 * SUM(b.H2B) + 2.0 * SUM(b.H3B)
                                         + 3.0 * SUM(b.HR)) / sum(b.AB) as lslg
  from people as p inner join batting as b on p.playerID = b.playerID
  group by p.playerID, namefirst, namelast
  having sum(b.AB) > 50 and (SUM(b.H) + 1.0 * SUM(b.H2B) + 2.0 * SUM(b.H3B)
                                      + 3.0 * SUM(b.HR)) / sum(b.AB) > (
                                      select (SUM(bb.H) + 1.0 * SUM(bb.H2B) + 2.0 * SUM(bb.H3B)
                                         + 3.0 * SUM(bb.HR)) / sum(bb.AB) as ll
                                      from batting as bb
                                      where bb.playerID = 'mayswi01'
                                      group by bb.playerID
                                      )
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT s.yearID, min(salary), max(salary), avg(salary)
  from people as p inner join salaries as s on p.playerID = s.playerID
  group by s.yearID
  order by s.yearID asc
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
with
range as (
    select
        min (salary) as min_salary,
        max (salary) as max_salary
    from salaries
    where yearID = 2016
)

select binid, low, high, count(*)
from
(
SELECT
    CAST((salary- min_salary) / ((max_salary - min_salary) / 10.0) as INT) as binid,

    min_salary +
    CAST((salary - min_salary) / ((max_salary - min_salary) / 10.0) as INT) * CAST(((max_salary - min_salary) / 10.0) as INT) as low,

    min_salary +
    (CAST((salary - min_salary) / ((max_salary - min_salary) / 10.0) as INT) +1)* CAST(((max_salary - min_salary) / 10.0) as INT) as high

from salaries cross join range -- 列级别的操作！
where salaries.yearID = 2016 and salary < 33000000.0

union all

select
    9 as binid,
    29750750.0 as low,
    max(salary) as high

from salaries
where salaries.yearID = 2016 and salary >= 29750750.0
)
-- in prepare, do not use agg functions！
-- using SET operations can be a good idea!

group by binid, low, high
order by binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
--  SELECT s1.yearID, min(s1.salary) - min(s2.salary), max(s1.salary) - max(s2.salary), avg(s1.salary) - avg(s2.salary)
--  from salaries as s1 left outer join salaries as s2 on s1.yearID = s2.yearID + 1
--  where s1.yearID > 1985
--  group by s1.yearID
--  order by s1.yearID asc
--; -- 注意上面的有问题！会loop卡住
WITH yearly_stats AS (
    SELECT
        yearID,
        MIN(salary) AS min_salary,
        MAX(salary) AS max_salary,
        AVG(salary) AS avg_salary
    FROM salaries
    GROUP BY yearID
)
SELECT
    s1.yearID,
    s1.min_salary - s2.min_salary AS mindiff,
    s1.max_salary - s2.max_salary AS maxdiff,
    s1.avg_salary - s2.avg_salary AS avgdiff
FROM yearly_stats s1
JOIN yearly_stats s2 ON s1.yearID = s2.yearID + 1
ORDER BY s1.yearID ASC; -- 先聚类后差分计算

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
select p.playerID as playerid, namefirst, namelast, salary, yearID as yearid
from people as p inner join salaries as s on p.playerID = s.playerID
where (yearid = 2000 and salary = (select max(salary) from salaries where yearID = 2000)) or (yearid = 2001 and salary = (select max(salary) from salaries where yearID = 2001))
-- 在于条件嵌套
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
select a.teamid as team, max(salary) - min (salary) as diffAvg
from allstarfull as a inner join salaries as s on s.playerid = a.playerid
where s.yearid = 2016 and a.yearid = 2016
group by a.teamid
;

-- time-stamp: 02h06m40s + 01h00m00s

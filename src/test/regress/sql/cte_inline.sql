CREATE SCHEMA cte_inline;
SET search_path TO cte_inline;
SET citus.next_shard_id TO 1960000;
CREATE TABLE test_table (key int, value text, other_value jsonb);
SELECT create_distributed_table ('test_table', 'key');

SET client_min_messages TO DEBUG;

-- Citus should not inline this CTE because otherwise it cannot
-- plan the query
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	*, (SELECT 1)
FROM
	cte_1;

-- the cte can be inlined because the unsupported
-- part of the query (subquery in WHERE clause)
-- doesn't access the cte
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1
WHERE
	key IN (
			SELECT
				(SELECT 1)
			FROM
				test_table WHERE key = 1
			);

-- a similar query as the above, and this time the planning
-- fails, but it fails because the subquery in WHERE clause
-- cannot be planned by Citus
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1
WHERE
	key IN (
			SELECT
				key
			FROM
				test_table
  			 	FOR UPDATE
			);

-- even if the CTE is not used immediately
-- on a query that Citus could not support,
-- it skips inlining, otherwise the planner
-- might fail
WITH cte_1 AS
  (SELECT *
   FROM test_table)
SELECT *, (SELECT 1)
FROM
  (SELECT *
   FROM cte_1) AS foo;

-- a little more complicated query tree
-- Citus can inline top_cte, because when inlined
-- it can be planned by Citus. However, citus doesn't
-- inline cte_1 because when inlined, Citus cannot
-- plan the query
WITH top_cte AS
  (SELECT *
   FROM test_table)
SELECT *
FROM top_cte,
  (WITH cte_1 AS
     (SELECT *
      FROM test_table) SELECT *, (SELECT 1)
   FROM
     (SELECT *
      FROM cte_1) AS foo) AS bar;

-- CTE is used inside a subquery in WHERE clause
-- so, should not be inlined
WITH cte_1 AS
  (SELECT *
   FROM test_table)
SELECT count(*)
FROM test_table
WHERE KEY IN
    (SELECT (SELECT 1)
     FROM
       (SELECT *,
               random()
        FROM
          (SELECT *
           FROM cte_1) AS foo) AS bar);

-- cte_1 is used inside another CTE, but still
-- should not be inlined because it is finally
-- used in an unsupported query
WITH cte_1 AS
  (SELECT *
   FROM test_table)
SELECT (SELECT 1) AS KEY  FROM (
  WITH cte_2 AS (SELECT *, random()
     FROM (SELECT *,random() FROM cte_1) as foo)
SELECT *, random() FROM cte_2) as bar;

-- in this example, cte_2 can be inlined, because it is not used
-- on any query that Citus cannot plan. However, cte_1 should not be
-- inlined, because it is used with a subquery in target list
WITH cte_1 AS (SELECT * FROM test_table),
     cte_2 AS (select * from test_table)
SELECT
	*
FROM
	(SELECT *, (SELECT 1) FROM cte_1) as foo
		JOIN
	cte_2
		ON (true);

-- unreferenced CTEs are just ignored
-- by Citus/Postgres
WITH a AS (SELECT * FROM test_table)
SELECT
	*, row_number() OVER ()
FROM
	test_table
WHERE
	key = 1;

-- router queries are affected by the distributed
-- cte inlining. In this example, although it is
-- a router query, Citus decides not to inline the CTE
-- because of the subquery in target list
WITH a AS (SELECT * FROM test_table WHERE key = 1)
SELECT
	*, (SELECT 1)
FROM
	a
WHERE
	key = 1;

-- router queries are affected by the distributed
-- cte inlining as well
WITH a AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	a
WHERE
	key = 1;

-- citus should not inline the CTE because it is used multiple times
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1 as first_entry
		JOIN
	cte_1 as second_entry
		USING (key);

-- ctes with volatile functions are not
-- inlined
WITH cte_1 AS (SELECT *, random() FROM test_table)
SELECT
	*
FROM
	cte_1;

-- cte_1 should be able to inlined even if
-- it is used one level below
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	*
FROM
(
	WITH ct2 AS (SELECT * FROM cte_1)
	SELECT * FROM ct2
) as foo;

-- a similar query, but there is also
-- one more cte, which relies on the previous
-- CTE
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	*
FROM
(
	WITH cte_2 AS (SELECT * FROM cte_1),
		 cte_3 AS (SELECT * FROM cte_2)
	SELECT * FROM cte_3
) as foo;


-- inlined CTE contains a reference to outer query
-- should be fine (because we pushdown the whole query)
SELECT *
	FROM
	  (SELECT *
	   FROM test_table) AS test_table_cte
	JOIN LATERAL
	  (WITH bar AS  (SELECT *
	      FROM test_table
	      WHERE key = test_table_cte.key)
	  	SELECT *
	   FROM
	      bar
	   LEFT JOIN test_table u2 ON u2.key = bar.key) AS foo ON TRUE;

-- inlined CTE contains a reference to outer query
-- should be fine (even if the recursive planning fails
-- to recursively plan the query)
SELECT *
	FROM
	  (SELECT *
	   FROM test_table) AS test_table_cte
	JOIN LATERAL
	  (WITH bar AS  (SELECT *
	      FROM test_table
	      WHERE key = test_table_cte.key)
	  	SELECT *
	   FROM
	      bar
	   LEFT JOIN test_table u2 ON u2.key = bar.value::int) AS foo ON TRUE;


-- inlined CTE can recursively planned later, that's the decision
-- recursive planning makes
-- LIMIT 5 in cte2 triggers recusrive planning, after cte inlining
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	*
FROM
(
	WITH ct2 AS (SELECT * FROM cte_1 LIMIT 5)
	SELECT * FROM ct2
) as foo;

-- all nested CTEs can be inlinied
WITH cte_1  AS (
  WITH cte_1 AS (
    WITH cte_1 AS (
      WITH cte_1 AS (
        WITH cte_1 AS (
          WITH cte_1 AS (
            WITH cte_1 AS (SELECT count(*), key FROM  test_table GROUP BY key)
            			   SELECT * FROM cte_1)
          SELECT * FROM cte_1 WHERE key = 1)
        SELECT * FROM cte_1 WHERE key = 2)
      SELECT * FROM cte_1 WHERE key = 3)
    SELECT * FROM cte_1 WHERE key = 4)
  SELECT * FROM cte_1 WHERE key = 5)
SELECT * FROM cte_1 WHERE key = 6;



-- ctes can be inlined even if they are used
-- in set operations
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
(SELECT * FROM cte_1 EXCEPT SELECT * FROM test_table)
UNION
(SELECT * FROM cte_2);

-- inlinining enforces the same restrictions
-- when the cte is used in set operations
-- so, cte_1 is not going to be inlined
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
(SELECT *, (SELECT 1) FROM cte_1 EXCEPT SELECT *, 1 FROM test_table)
UNION
(SELECT *, 1 FROM cte_2);


-- cte_1 is not safe to inline, because after inlining
-- it'd be in a query tree where there is a query that is
-- not supported by Citus
-- cte_2 is on another queryTree, should be fine
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
(SELECT *, (SELECT key FROM cte_1) FROM test_table)
UNION
(SELECT *, 1 FROM cte_2);

-- after inlining CTEs, the query becomes
-- subquery pushdown with set operations
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
SELECT * FROM
(
	SELECT * FROM cte_1
		UNION
	SELECT * FROM cte_2
) as bar;

-- cte LEFT JOIN subquery should only work
-- when CTE is inlined, as Citus currently
-- doesn't know how to handle intermediate
-- results in the outer parts of outer
-- queries
WITH cte AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte LEFT JOIN test_table USING (key);

-- the CTEs are very simple, so postgres
-- can pull-up the subqueries after inlining
-- the CTEs, and the query that we send to workers
-- becomes a join between two tables
WITH cte_1 AS (SELECT key FROM test_table),
	 cte_2 AS (SELECT key FROM test_table)
SELECT
	count(*)
FROM
	cte_1 JOIN cte_2 USING (key);


-- the following query is kind of interesting
-- During INSERT .. SELECT via coordinator,
-- Citus moves the CTEs into SELECT part, and plans/execute
-- the SELECT separately. Thus, fist_table_cte can be inlined
-- by Citus -- but not by Postgres
WITH fist_table_cte AS
  (SELECT * FROM test_table)
INSERT INTO test_table
            (key, value)
            SELECT
              key, value
            FROM
              fist_table_cte;

-- the following INSERT..SELECT is even more interesting
-- the CTE becomes
-- TODO: discuss with Marco, is there anything that we should
-- prevent this? Postgres doesn't inline CTEs in modification queries
-- but why?
INSERT INTO test_table
WITH fist_table_cte AS
  (SELECT * FROM test_table)
    SELECT
      key, value
    FROM
      fist_table_cte;

-- update/delete/modifying ctes
-- we don't support any cte inlining in modifications
-- queries and modifying CTEs
WITH cte_1 AS (SELECT * FROM test_table)
	DELETE FROM test_table WHERE key NOT IN (SELECT key FROM cte_1);

-- we don't inline CTEs if they are modifying CTEs
WITH cte_1 AS (DELETE FROM test_table RETURNING key)
SELECT * FROM cte_1;

-- cte with column aliases
SELECT * FROM test_table,
(WITH cte_1 (x,y) AS (SELECT * FROM test_table),
     cte_2 (z,y) AS (SELECT value, other_value, key FROM test_table),
	 cte_3 (t,m) AS (SELECT z, y, key as cte_2_key FROM cte_2)
		SELECT * FROM cte_2, cte_3) as bar;

-- cte used in HAVING subquery just works fine
-- even if it is inlined
WITH cte_1 AS (SELECT max(key) as max FROM test_table)
SELECT
	key, count(*)
FROM
	test_table
GROUP BY
	key
HAVING
	(count(*) > (SELECT max FROM cte_1));

-- cte used in ORDER BY just works fine
-- even if it is inlined
WITH cte_1 AS (SELECT max(key) as max FROM test_table)
SELECT
	key
FROM
	test_table JOIN cte_1 ON (key = max)
ORDER BY
	cte_1.max;

PREPARE inlined_cte_without_params AS
	WITH cte_1 AS (SELECT count(*) FROM test_table GROUP BY key)
	SELECT * FROM cte_1;
PREPARE inlined_cte_has_parameter_on_non_dist_key(int) AS
	WITH cte_1 AS (SELECT count(*) FROM test_table WHERE value::int = $1 GROUP BY key)
	SELECT * FROM cte_1;
PREPARE inlined_cte_has_parameter_on_dist_key(int) AS
	WITH cte_1 AS (SELECT count(*) FROM test_table WHERE key > $1 GROUP BY key)
	SELECT * FROM cte_1;

EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;

EXECUTE inlined_cte_has_parameter_on_non_dist_key(1);
EXECUTE inlined_cte_has_parameter_on_non_dist_key(2);
EXECUTE inlined_cte_has_parameter_on_non_dist_key(3);
EXECUTE inlined_cte_has_parameter_on_non_dist_key(4);
EXECUTE inlined_cte_has_parameter_on_non_dist_key(5);
EXECUTE inlined_cte_has_parameter_on_non_dist_key(6);

EXECUTE inlined_cte_has_parameter_on_dist_key(1);
EXECUTE inlined_cte_has_parameter_on_dist_key(2);
EXECUTE inlined_cte_has_parameter_on_dist_key(3);
EXECUTE inlined_cte_has_parameter_on_dist_key(4);
EXECUTE inlined_cte_has_parameter_on_dist_key(5);
EXECUTE inlined_cte_has_parameter_on_dist_key(6);

-- prevent DROP CASCADE to give notices
SET client_min_messages TO ERROR;
DROP SCHEMA cte_inline CASCADE;

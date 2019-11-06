/*-------------------------------------------------------------------------
 *
 * cte_inline.c
 *	  For multi-shard queries, Citus can only recursively plan CTEs. Instead,
 *	  with the functions defined in this file, the certain CTEs can be inlined
 *	  as subqueries in the query tree. In that case, more optimal distributed
 *	  planning, the query pushdown planning, kicks in and the CTEs can actually
 *	  be pushed down as long as it is safe to pushdown as a subquery.
 *
 *	  Most of the logic in this function is inspired (and some is copy & pasted)
 *	  from PostgreSQL 12's CTE inlining feature.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/cte_inline.h"
#include "distributed/multi_logical_optimizer.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#endif
#include "rewrite/rewriteManip.h"

#if PG_VERSION_NUM < 120000

/* copy & paste from PG 12 */
#define PG_12_QTW_EXAMINE_RTES_BEFORE 0x10
#define PG_12_QTW_EXAMINE_RTES_AFTER 0x20
bool pg_12_query_tree_walker(Query *query,
							 bool (*walker)(),
							 void *context,
							 int flags);
bool pg_12_range_table_walker(List *rtable,
							  bool (*walker)(),
							  void *context,
							  int flags);
#endif

typedef struct inline_cte_walker_context
{
	const char *ctename;       /* name and relative level of target CTE */
	int levelsup;
	int refcount;              /* number of remaining references */
	Query *ctequery;           /* query to substitute */

	List *aliascolnames;  /* citus addition to Postgres' inline_cte_walker_context */
} inline_cte_walker_context;


/* copy & paste from Postgres source, moved into a function for readability */
static bool PostgreSQLCTEInlineCondition(CommonTableExpr *cte, CmdType cmdType);

/* the following utility functions are copy & paste from PostgreSQL code */
static bool inline_cte_walker(Node *node, inline_cte_walker_context *context);
static void inline_cte(Query *mainQuery, CommonTableExpr *cte);
static bool contain_dml(Node *node);
static bool contain_dml_walker(Node *node, void *context);

/* the following utility functions are related to Citus' logic */
static bool RecusrivelyInlineCteWalker(Node *node, void *context);
static void InlineCTEsInQueryTree(Query *query);
static Query * QueryInWhichCteIsUsed(Query *query, CommonTableExpr *cte);
static bool CteUsedInRtable(List *rangeTableList, CommonTableExpr *cte);
static bool QueryTreeContainsCteWalker(Node *node);


/*
 * RecursivelyInlineCtesInQueryTree gets a query and recursively traverses the
 * tree from top to bottom. On each level, the CTEs that are eligable for
 * inlining are inlined as subqueries. This is useful in distributed planning
 * because Citus' sub(query) planning logic superior to CTE planning, where CTEs
 * are always recursively planned, which might produce very slow executions.
 */
void
RecursivelyInlineCtesInQueryTree(Query *query)
{
	InlineCTEsInQueryTree(query);

	query_tree_walker(query, RecusrivelyInlineCteWalker, NULL, 0);
}


/*
 * RecusrivelyInlineCteWalker recursively finds all the Query nodes and
 * recursively plans if necessary.
 */
static bool
RecusrivelyInlineCteWalker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		InlineCTEsInQueryTree(query);

		query_tree_walker(query, RecusrivelyInlineCteWalker, NULL, 0);

		/* we're done, no need to recurse anymore for this query */
		return false;
	}

	return expression_tree_walker(node, RecusrivelyInlineCteWalker, context);
}


/*
 * InlineCTEsInQueryTree gets a query tree and tries to inline CTEs as subqueries
 * in the query tree.
 *
 * Most of the code is coming from PostgreSQL's CTE inlining logic. On top of the rules
 * that PostgreSQL enforces before inlining CTEs, Citus adds one more check. The check is
 * that if a CTE is inlined, would the resulting query become plannable by Citus? If not,
 * we skip inlining, and let the recursive planning handle it by converting the CTE to an
 * intermediate result, which always ends-up with a successfull distributed plan.
 */
void
InlineCTEsInQueryTree(Query *query)
{
	if (query->cteList == NULL || query->hasRecursive || query->hasModifyingCTE)
	{
		return;
	}

	ListCell *cteCell = NULL;

	/* iterate on the copy of the list because we'll be modifying query->cteList */
	List *copyOfCteList = list_copy(query->cteList);
	foreach(cteCell, copyOfCteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);

		/*
		 * First, make sure that Postgres is OK to inline the CTE. Later, check for
		 * distributed query planning constraints that might prevent inlining.
		 *
		 * We're conservative about inlining CTE(s) if the resulting query cannot be
		 * planned by Citus. The main idea here is that the user is likely to rely on
		 * recursive planning of the CTE(s) to be able to run the query such as the
		 * following where Citus cannot plan some of the window functions on distributed
		 * tables:
		 *
		 *    WITH cte_1 AS (SELECT * FROM test)
		 *      SELECT *, row_number() OVER () FROM cte_1;
		 */
		if (PostgreSQLCTEInlineCondition(cte, query->commandType))
		{
			Query *queryInWhichCteIsUsed = QueryInWhichCteIsUsed(query, cte);

			/*
			 * Although logical planner cannot handle CTEs (e.g., the following check
			 * would fail on any query with CTEs), Citus has other ways of planning CTEs.
			 * We'd either inline CTEs here, or recursively plan. So, ignore CTEs for the
			 * purposes of this check.
			 */
			List *cteListOfQueryWhereCteIsUsed = queryInWhichCteIsUsed->cteList;
			queryInWhichCteIsUsed->cteList = NIL;
			DeferredErrorMessage *deferredError =
				DeferErrorIfQueryNotSupported(queryInWhichCteIsUsed);
			if (deferredError != NULL)
			{
#if PG_VERSION_NUM <= 110000

				/*
				 *  A comment block within PG_VERSION_NUM check, interesting ha?
				 *  Please read through especially if you're dropping support for
				 *  Postgres 11.
				 *
				 *  In Postgres 12 (and above), Postgres does the CTE inlining
				 *  during standard_planner(). However, as of now, Citus supports
				 *  Postgres 11 as well. Thus, Citus takes care of CTE inlining
				 *  even before standard planner is called so that Citus behaves
				 *  the same across different postgres versions.
				 *
				 *  Because we inline CTEs before any distributed planning, this
				 *  check is unnecessarily enforced for router queries as well.
				 *  That's not a problem because router planner can handle
				 *  subqueries and CTEs equally. But, we unnecessarily have to
				 *  go through these code-path.
				 *
				 *  In that sense, when we drop Postgres 11  support, we should
				 *  remove RecursivelyInlineCtesInQueryTree() function from the
				 *  source code and move InlineCTEsInQueryTree() to
				 *  RecursivelyPlanSubqueriesAndCTEs() just before CTEs are
				 *  recursively planned. That would prevent router queries go
				 *  through this logic and prevent an extra query tree traversal/copy
				 *  at the start of the distributed planning.
				 */
#endif
				queryInWhichCteIsUsed->cteList = cteListOfQueryWhereCteIsUsed;
				elog(INFO, "Skipped inlining the cte %s because if inlined, "
						   "Citus planner would error with: %s", cte->ctename,
					 deferredError->message);
				continue;
			}

			/* set the original cteList back */
			queryInWhichCteIsUsed->cteList = cteListOfQueryWhereCteIsUsed;

			elog(DEBUG1, "CTE %s is going to be inlined via "
						 "distributed planning", cte->ctename);

			/* do the hard work of cte inlining */
			inline_cte(query, cte);

			/* clean-up the necessary fields */
			cte->cterefcount = 0;
			query->cteList = list_delete_ptr(query->cteList, cte);
		}
	}
}


/*
 * QueryInWhichCteIsUsed gets a Query and CommonTableExpr, traverses all the
 * (sub)queries in the query, and returns the query where the cte is in the
 * range table entry.
 *
 * The function never returns NULL, but it could throw an error, see the details
 * in the function.
 */
static Query *
QueryInWhichCteIsUsed(Query *query, CommonTableExpr *cte)
{
	/*
	 * Given that PostgreSQLCTEInlineCondition() checks for cte->cterefcount == 1,
	 * we don't have to deal with ctelevels while finding the query that the cte
	 * is used since any inlining candidate can only be used once in the query.
	 */
	Assert(cte->cterefcount == 1);

	ListCell *queryCell = NULL;
	List *allQueries = NIL;

	/*
	 * ExtractQueryWalker pulls the (sub)queries in the query from top to bottom.
	 * This is important because there might be ctes with the same name as the
	 * input cte, defined in the lower parts of the query. With this approach, we're
	 * ensuring that we'll find the cte that we're interested in.
	 */
	ExtractQueryWalker((Node *) query, &allQueries);
	Assert(equal(query, linitial(allQueries)));
	foreach(queryCell, allQueries)
	{
		Query *aQuery = (Query *) lfirst(queryCell);
		if (CteUsedInRtable(aQuery->rtable, cte))
		{
			return aQuery;
		}
	}

	/*
	 * We should never get here, unless the query is bogus (which is unlikely) or
	 * we're calling the wrong query/cte pair.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("could not find cte: %s in the query",
					cte->ctename)));
}


/*
 * CteUsedInRtable gets a rangeTableList and a CommonTableExpr. The
 * function returns true if the cte shows up in the range table list.
 *
 *
 * Also, the function does not consider ctelevelsup, that's the callers
 * responsibility to make sure the relevant CTEs are passed to the function.
 */
static bool
CteUsedInRtable(List *rangeTableList, CommonTableExpr *cte)
{
	ListCell *rteCell = NULL;

	foreach(rteCell, rangeTableList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind == RTE_CTE && strcmp(rte->ctename, cte->ctename) == 0)
		{
			return true;
		}
	}
	return false;
}


/*
 * PostgreSQLCTEInlineCondition returns true if the CTE is considered
 * safe to inline by Postgres.
 */
static bool
PostgreSQLCTEInlineCondition(CommonTableExpr *cte, CmdType cmdType)
{
	if (cte->cterefcount == 1 &&
		!cte->cterecursive &&
		cmdType == CMD_SELECT &&
		!contain_dml(cte->ctequery) &&
		!contain_volatile_functions(cte->ctequery) &&
#if PG_VERSION_NUM >= 120000
		(cte->ctematerialized == CTEMaterializeNever ||
		 cte->ctematerialized == CTEMaterializeDefault))
#else
		true) /* inlining performs better, for pg < 12, try inlining */
#endif
	{
		return true;
	}

	return false;
}


/* *INDENT-OFF* */
/*
 * inline_cte: convert RTE_CTE references to given CTE into RTE_SUBQUERYs
 */
static void
inline_cte(Query *mainQuery, CommonTableExpr *cte)
{
	struct inline_cte_walker_context context;

	context.ctename = cte->ctename;

	/* Start at levelsup = -1 because we'll immediately increment it */
	context.levelsup = -1;
	context.refcount = cte->cterefcount;
	context.ctequery = castNode(Query, cte->ctequery);
	context.aliascolnames = cte->aliascolnames;

	(void) inline_cte_walker((Node *) mainQuery, &context);

	/* Assert we replaced all references */
	Assert(context.refcount == 0);
}

/*
 * See PostgreSQL's source code at src/backend/optimizer/plan/subselect.c.
 */
static bool
inline_cte_walker(Node *node, inline_cte_walker_context *context)
{
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		context->levelsup++;

		/*
		 * Visit the query's RTE nodes after their contents; otherwise
		 * query_tree_walker would descend into the newly inlined CTE query,
		 * which we don't want.
		 */
#if PG_VERSION_NUM < 120000
		(void) pg_12_query_tree_walker(query, inline_cte_walker, context,
									   PG_12_QTW_EXAMINE_RTES_AFTER);
#else
		(void) query_tree_walker(query, inline_cte_walker, context,
								 QTW_EXAMINE_RTES_AFTER);
#endif
		context->levelsup--;

		return false;
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_CTE &&
			strcmp(rte->ctename, context->ctename) == 0 &&
			rte->ctelevelsup == context->levelsup)
		{
			/*
			 * Found a reference to replace.  Generate a copy of the CTE query
			 * with appropriate level adjustment for outer references (e.g.,
			 * to other CTEs).
			 */
			Query *newquery = copyObject(context->ctequery);

			if (context->levelsup > 0)
			{
				IncrementVarSublevelsUp((Node *) newquery, context->levelsup, 1);
			}

			/*
			 * Convert the RTE_CTE RTE into a RTE_SUBQUERY.
			 *
			 * Historically, a FOR UPDATE clause has been treated as extending
			 * into views and subqueries, but not into CTEs.  We preserve this
			 * distinction by not trying to push rowmarks into the new
			 * subquery.
			 */
			rte->rtekind = RTE_SUBQUERY;
			rte->subquery = newquery;
			rte->security_barrier = false;

			/*
			 * This part is Ctus addition to handle CTEs with aliases. We do something similar
			 * in recursive CTE planning as well.
			 */
			List *columnAliasList = context->aliascolnames;
			int columnAliasCount = list_length(columnAliasList);
			int columnNumber = 1;
			for (; columnNumber < list_length(rte->subquery->targetList) + 1;
				 ++columnNumber)
			{
				if (columnAliasCount >= columnNumber)
				{
					Value *columnAlias = (Value *) list_nth(columnAliasList,
															columnNumber - 1);

					TargetEntry *targetEntry =
						list_nth(rte->subquery->targetList, columnNumber - 1);
					Assert(IsA(columnAlias, String));

					targetEntry->resname = strVal(columnAlias);
				}
			}

			/* Zero out CTE-specific fields */
			rte->ctename = NULL;
			rte->ctelevelsup = 0;
			rte->self_reference = false;
			rte->coltypes = NIL;
			rte->coltypmods = NIL;
			rte->colcollations = NIL;

			/* Count the number of replacements we've done */
			context->refcount--;
		}

		return false;
	}

	return expression_tree_walker(node, inline_cte_walker, context);
}


/*
 * contain_dml: is any subquery not a plain SELECT?
 *
 * We reject SELECT FOR UPDATE/SHARE as well as INSERT etc.
 */
static bool
contain_dml(Node *node)
{
	return contain_dml_walker(node, NULL);
}


static bool
contain_dml_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		if (query->commandType != CMD_SELECT ||
			query->rowMarks != NIL)
		{
			return true;
		}

		return query_tree_walker(query, contain_dml_walker, context, 0);
	}
	return expression_tree_walker(node, contain_dml_walker, context);
}


#if PG_VERSION_NUM < 120000
/*
 * pg_12_query_tree_walker is copied from Postgres 12's source
 * code. The only difference between query_tree_walker the new
 * two flags added in range_table_walker: QTW_EXAMINE_RTES_AFTER
 * and QTW_EXAMINE_RTES_BEFORE.
 */
bool
pg_12_query_tree_walker(Query *query,
				  bool (*walker) (),
				  void *context,
				  int flags)
{
	Assert(query != NULL && IsA(query, Query));

	if (walker((Node *) query->targetList, context))
		return true;
	if (walker((Node *) query->withCheckOptions, context))
		return true;
	if (walker((Node *) query->onConflict, context))
		return true;
	if (walker((Node *) query->returningList, context))
		return true;
	if (walker((Node *) query->jointree, context))
		return true;
	if (walker(query->setOperations, context))
		return true;
	if (walker(query->havingQual, context))
		return true;
	if (walker(query->limitOffset, context))
		return true;
	if (walker(query->limitCount, context))
		return true;
	if (!(flags & QTW_IGNORE_CTE_SUBQUERIES))
	{
		if (walker((Node *) query->cteList, context))
			return true;
	}
	if (!(flags & QTW_IGNORE_RANGE_TABLE))
	{
		if (pg_12_range_table_walker(query->rtable, walker, context, flags))
			return true;
	}
	return false;
}

/*
 * pg_12_range_table_walker is copied from Postgres 12's source
 * code. The only difference between range_table_walker the new
 * two flags added in range_table_walker: QTW_EXAMINE_RTES_AFTER
 * and QTW_EXAMINE_RTES_BEFORE.
 */
bool
pg_12_range_table_walker(List *rtable,
				   bool (*walker) (),
				   void *context,
				   int flags)
{
	ListCell   *rt;

	foreach(rt, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

		/*
		 * Walkers might need to examine the RTE node itself either before or
		 * after visiting its contents (or, conceivably, both).  Note that if
		 * you specify neither flag, the walker won't visit the RTE at all.
		 */
		if (flags & PG_12_QTW_EXAMINE_RTES_BEFORE)
			if (walker(rte, context))
				return true;

		switch (rte->rtekind)
		{
			case RTE_RELATION:
				if (walker(rte->tablesample, context))
					return true;
				break;
			case RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
				/* nothing to do */
				break;
			case RTE_SUBQUERY:
				if (!(flags & QTW_IGNORE_RT_SUBQUERIES))
					if (walker(rte->subquery, context))
						return true;
				break;
			case RTE_JOIN:
				if (!(flags & QTW_IGNORE_JOINALIASES))
					if (walker(rte->joinaliasvars, context))
						return true;
				break;
			case RTE_FUNCTION:
				if (walker(rte->functions, context))
					return true;
				break;
			case RTE_TABLEFUNC:
				if (walker(rte->tablefunc, context))
					return true;
				break;
			case RTE_VALUES:
				if (walker(rte->values_lists, context))
					return true;
				break;
		}

		if (walker(rte->securityQuals, context))
			return true;

		if (flags & PG_12_QTW_EXAMINE_RTES_AFTER)
			if (walker(rte, context))
				return true;
	}
	return false;
}
#endif

/* *INDENT-ON* */

/*
 * QueryTreeContainsCTE recursively traverses the queryTree, and returns true
 * if any of the (sub)queries in the queryTree contains at least one CTE.
 */
bool
QueryTreeContainsCTE(Query *queryTree)
{
	return QueryTreeContainsCteWalker((Node *) queryTree);
}


/*
 * QueryTreeContainsCteWalker walks over the node, and returns true if any of
 * the (sub)queries in the node contains at least one CTE.
 */
static bool
QueryTreeContainsCteWalker(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		if (query->cteList != NIL)
		{
			return true;
		}

		return query_tree_walker(query, QueryTreeContainsCteWalker, NULL, 0);
	}

	return expression_tree_walker(node, QueryTreeContainsCteWalker, NULL);
}

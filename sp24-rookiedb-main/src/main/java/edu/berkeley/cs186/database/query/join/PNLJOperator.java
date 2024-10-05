package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.query.QueryOperator;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Page Nested Loop Join algorithm.
 * note: PNLJ has already been implemented for you as a special case of BNLJ
 * with B=3. Therefore, it will not function properly until BNLJ has been properly
 * implemented.
 */
public class PNLJOperator extends BNLJOperator {
    public PNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction);

        joinType = JoinType.PNLJ;
        numBuffers = 3; // B > 2 !!
    }
}

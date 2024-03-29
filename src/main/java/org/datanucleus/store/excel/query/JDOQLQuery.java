/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
2008 Andy Jefferson - logging, checking for sheet existence
    ...
***********************************************************************/
package org.datanucleus.store.excel.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.inmemory.JDOQLInMemoryEvaluator;
import org.datanucleus.store.query.inmemory.JavaQueryInMemoryEvaluator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * JDOQL query for Excel documents.
 * Retrieves all objects in the worksheet of the candidate class, and then applies the
 * generic JDOQLEvaluator to apply the filter, result, grouping, ordering etc.
 */
public class JDOQLQuery extends AbstractJDOQLQuery
{
    private static final long serialVersionUID = 1604006277815191945L;

    /**
     * Constructs a new query instance that uses the given execution context.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JDOQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, JDOQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    /**
     * Method to execute the query, specific to Excel datastores.
     * Here we retrieve all objects of the candidate type (from POI), and process them using
     * the in-memory evaluator.
     * @param parameters Map of parameter values keyed by name.
     */
    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", QueryLanguage.JDOQL.name(), getSingleStringQuery(), null));
            }

            List candidates = null;
            if (candidateCollection == null)
            {
                candidates = new ExcelCandidateList(candidateClass, subclasses, ec, (String)getExtension(Query.EXTENSION_RESULT_CACHE_TYPE), mconn, ignoreCache, getFetchPlan());
            }
            else
            {
                candidates = new ArrayList(candidateCollection);
            }

            // Evaluate result/filter/grouping/having/ordering in-memory
            JavaQueryInMemoryEvaluator resultMapper = new JDOQLInMemoryEvaluator(this, candidates, compilation,
                parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", QueryLanguage.JDOQL.name(), "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == QueryType.BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == QueryType.BULK_UPDATE)
            {
                // TODO Support BULK UPDATE
                throw new NucleusException("Bulk Update is not yet supported");
            }
            else
            {
                return results;
            }
        }
        finally
        {
            mconn.release();
        }
    }
}
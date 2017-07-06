/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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
 ...
***********************************************************************/
package org.datanucleus.store.excel;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.excel.query.JDOQLQuery;
import org.datanucleus.store.excel.query.JPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;

/**
 * StoreManager for data access to Excel documents (XSL or OOXML).
 * Makes use of Apache POI project.
 */
public abstract class ExcelStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    static
    {
        // Register our localisation
        Localiser.registerBundle("org.datanucleus.store.excel.Localisation", ExcelStoreManager.class.getClassLoader());
    }

    public ExcelStoreManager(String storeMgrKey, ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super(storeMgrKey, clr, ctx, props);

        // Check if Apache POI JAR is in CLASSPATH
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.apache.poi.hssf.usermodel.HSSFWorkbook", "poi.jar");

        schemaHandler = new ExcelSchemaHandler(this);
        persistenceHandler = new ExcelPersistenceHandler(this);

        logConfiguration();
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_NONDURABLE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
        set.add(StoreManager.OPTION_ORM_INHERITANCE_COMPLETE_TABLE);
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equalsIgnoreCase("JDOQL"))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equalsIgnoreCase("JPQL"))
        {
            return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equalsIgnoreCase("JDOQL"))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equalsIgnoreCase("JPQL"))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equalsIgnoreCase("JDOQL"))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equalsIgnoreCase("JPQL"))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = connectionMgr.getConnection(-1);
        try
        {
            Workbook wb = (Workbook)mconn.getConnection();
            manageClasses(classNames, clr, wb);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, Workbook wb)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isEmbeddedOnly())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }

                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }

        // Create schema for classes
        schemaHandler.createSchemaForClasses(clsNameSet, null, wb);
    }

    public void createDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.createDatabase(catalogName, schemaName, props, null);
    }

    public void deleteDatabase(String catalogName, String schemaName, Properties props)
    {
        schemaHandler.deleteDatabase(catalogName, schemaName, props, null);
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.createSchemaForClasses(classNames, props, null);
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.deleteSchemaForClasses(classNames, props, null);
    }

    public void validateSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.validateSchema(classNames, props, null);
    }
}
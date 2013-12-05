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

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * StoreManager for data access to Excel documents (XSL or OOXML).
 * Makes use of Apache POI project.
 */
public abstract class ExcelStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    public ExcelStoreManager(String storeMgrKey, ClassLoaderResolver clr, NucleusContext ctx, Map<String, Object> props)
    {
        super(storeMgrKey, clr, ctx, props);

        // Check if Apache POI JAR is in CLASSPATH
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.apache.poi.hssf.usermodel.HSSFWorkbook", "poi.jar");

        persistenceHandler = new ExcelPersistenceHandler(this);

        logConfiguration();
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("NonDurableIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        set.add("ORM");
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#createSchema(java.util.Set, java.util.Properties)
     */
    public void createSchema(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            Workbook wb = (Workbook) mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    String sheetName = getNamingFactory().getTableName(cmd);
                    Sheet sheet = wb.getSheet(sheetName);
                    if (sheet == null)
                    {
                        // Sheet doesn't exist so create it
                        sheet = wb.createSheet(sheetName);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("Excel.SchemaCreate.Class",
                                cmd.getFullClassName(), sheetName));
                        }

                        // Create columns of sheet
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set)
     */
    public void deleteSchema(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            Workbook wb = (Workbook) mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    String sheetName = getNamingFactory().getTableName(cmd);
                    Sheet sheet = wb.getSheet(sheetName);
                    if (sheet != null)
                    {
                        wb.removeSheetAt(wb.getSheetIndex(sheetName));
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("Excel.SchemaDelete.Class",
                                cmd.getFullClassName(), sheetName));
                        }
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#validateSchema(java.util.Set)
     */
    public void validateSchema(Set<String> classNames, Properties props)
    {
        // TODO Auto-generated method stub
        
    }
}
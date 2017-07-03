/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.excel;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Handler for schema operations with Excel documents.
 */
public class ExcelSchemaHandler extends AbstractStoreSchemaHandler
{
    public ExcelSchemaHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#createSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        Workbook wb = (Workbook)connection;
        ManagedConnection mconn = null;
        try
        {
            if (wb == null)
            {
                mconn = storeMgr.getConnectionManager().getConnection(-1);
                wb = (Workbook)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                    Table table = null;
                    if (storeData != null)
                    {
                        table = storeData.getTable();
                    }
                    else
                    {
                        table = new CompleteClassTable(storeMgr, cmd, null);
                    }

                    String sheetName = table.getName();
                    Sheet sheet = wb.getSheet(sheetName);
                    if (sheet == null)
                    {
                        // Sheet doesn't exist so create it
                        sheet = wb.createSheet(sheetName);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.SchemaCreate.Class",
                                cmd.getFullClassName(), sheetName));
                        }

                        // Create columns of sheet
                        for (int i=0;i<table.getNumberOfColumns();i++)
                        {
                            // TODO Create header row
                        }
                    }
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#deleteSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void deleteSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        Workbook wb = (Workbook)connection;
        ManagedConnection mconn = null;
        try
        {
            if (wb == null)
            {
                mconn = storeMgr.getConnectionManager().getConnection(-1);
                wb = (Workbook)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                    Table table = null;
                    if (storeData != null)
                    {
                        table = storeData.getTable();
                    }
                    else
                    {
                        table = new CompleteClassTable(storeMgr, cmd, null);
                    }

                    String sheetName = table.getName();
                    Sheet sheet = wb.getSheet(sheetName);
                    if (sheet != null)
                    {
                        wb.removeSheetAt(wb.getSheetIndex(sheetName));
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.SchemaDelete.Class", cmd.getFullClassName(), sheetName));
                        }
                    }
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#validateSchema(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props, Object connection)
    {
        // TODO Implement validation of Excel spreadsheet
        super.validateSchema(classNames, props, connection);
    }
}
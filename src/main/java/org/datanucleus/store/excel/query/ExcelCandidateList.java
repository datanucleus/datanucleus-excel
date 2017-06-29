/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.excel.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.excel.ExcelStoreManager;
import org.datanucleus.store.excel.fieldmanager.FetchFieldManager;
import org.datanucleus.store.query.AbstractCandidateLazyLoadList;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;

/**
 * Wrapper for a List of candidate instances from Excel. Loads the instances from the workbook lazily.
 */
public class ExcelCandidateList extends AbstractCandidateLazyLoadList
{
    ManagedConnection mconn;

    boolean ignoreCache;

    FetchPlan fetchPlan;

    /** Number of objects per class, in same order as class meta-data. */
    List<Integer> numberInstancesPerClass = null;

    /**
     * Constructor for the lazy loaded Excel candidate list.
     * @param cls The candidate class
     * @param subclasses Whether to include subclasses
     * @param ec execution context
     * @param cacheType Type of caching
     * @param mconn Connection to the datastore
     * @param ignoreCache Whether to ignore the cache on object retrieval
     * @param fp Fetch Plan
     */
    public ExcelCandidateList(Class cls, boolean subclasses, ExecutionContext ec, String cacheType, ManagedConnection mconn, boolean ignoreCache, FetchPlan fp)
    {
        super(cls, subclasses, ec, cacheType);
        this.mconn = mconn;
        this.ignoreCache = ignoreCache;
        this.fetchPlan = fp;

        // Count the instances per class by scanning the associated worksheets
        numberInstancesPerClass = new ArrayList<Integer>();
        ExcelStoreManager storeMgr = (ExcelStoreManager) ec.getStoreManager();
        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        Workbook workbook = (Workbook) mconn.getConnection();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData cmd = cmdIter.next();

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), workbook);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            String sheetName = table.getName();
            Sheet sheet = workbook.getSheet(sheetName);
            int size = 0;
            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                // Take the next row in this worksheet
                int idColIndex = -1;
                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    int[] pkFieldNums = cmd.getPKMemberPositions(); // TODO Check all pk cols?
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[0]);
                    idColIndex = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getPosition();
                }
                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    idColIndex = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
                }
                else
                {
                    idColIndex = 0; // No id column with nondurable, so just take the first
                }

                for (int i=sheet.getFirstRowNum();i<=sheet.getLastRowNum();i++)
                {
                    Row row = sheet.getRow(i);
                    if (row.getCell(idColIndex) != null) // Omit inactive rows
                    {
                        size++;
                    }
                }
            }
            numberInstancesPerClass.add(size);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractLazyLoadList#getSize()
     */
    protected int getSize()
    {
        int size = 0;

        Iterator<Integer> numberIter = numberInstancesPerClass.iterator();
        while (numberIter.hasNext())
        {
            size += numberIter.next();
        }

        return size;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractLazyLoadList#retrieveObjectForIndex(int)
     */
    protected Object retrieveObjectForIndex(int index)
    {
        if (index < 0 || index >= getSize())
        {
            throw new NoSuchElementException();
        }

        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        Iterator<Integer> numIter = numberInstancesPerClass.iterator();
        int first = 0;
        int last = -1;
        while (cmdIter.hasNext())
        {
            final AbstractClassMetaData cmd = cmdIter.next();
            int number = numIter.next();
            last = first+number;

            if (index >= first && index < last)
            {
                // Object is of this candidate type, so find the object
                Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
                String sheetName = table.getName();
                Workbook workbook = (Workbook) mconn.getConnection();
                final Sheet worksheet = workbook.getSheet(sheetName);
                if (worksheet != null)
                {
                    int idColIndex = -1;
                    if (cmd.getIdentityType() == IdentityType.APPLICATION)
                    {
                        int[] pkFieldNums = cmd.getPKMemberPositions(); // TODO Check all pk cols?
                        AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNums[0]);
                        idColIndex = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getPosition();
                    }
                    else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        idColIndex = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
                    }
                    else
                    {
                        idColIndex = 0; // No id column with nondurable, so just take the first
                    }

                    int current = first;
                    for (int i=worksheet.getFirstRowNum();i<=worksheet.getLastRowNum();i++)
                    {
                        final Row row = worksheet.getRow(i);
                        if (row.getCell(idColIndex) != null) // Omit inactive rows
                        {
                            if (current == index)
                            {
                                // This row equates to the required index
                                final int rowNumber = i;
                                int[] fpFieldNums = fetchPlan.getFetchPlanForClass(cmd).getMemberNumbers();
                                if (cmd.getIdentityType() == IdentityType.APPLICATION)
                                {
                                    final FetchFieldManager fm = new FetchFieldManager(ec, cmd, worksheet, rowNumber, table);
                                    Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

                                    return ec.findObject(id, new FieldValues()
                                    {
                                        // ObjectProvider calls the fetchFields method
                                        public void fetchFields(ObjectProvider op)
                                        {
                                            op.replaceFields(fpFieldNums, fm);
                                        }
                                        public void fetchNonLoadedFields(ObjectProvider sm)
                                        {
                                            sm.replaceNonLoadedFields(fpFieldNums, fm);
                                        }
                                        public FetchPlan getFetchPlanForLoading()
                                        {
                                            return null;
                                        }
                                    }, null, ignoreCache, false);
                                }
                                else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                                {
                                    final FetchFieldManager fm = new FetchFieldManager(ec, cmd, worksheet, rowNumber, table);
                                    Object id = null;
                                    Cell idCell = row.getCell(idColIndex);
                                    int type = idCell.getCellType();
                                    if (type == Cell.CELL_TYPE_STRING)
                                    {
                                        String key = idCell.getRichStringCellValue().getString();
                                        id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), key);
                                    }
                                    else if (type == Cell.CELL_TYPE_NUMERIC)
                                    {
                                        long key = (long)idCell.getNumericCellValue();
                                        id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), key);
                                    }
                                    return ec.findObject(id, new FieldValues()
                                    {
                                        // ObjectProvider calls the fetchFields method
                                        public void fetchFields(ObjectProvider op)
                                        {
                                            op.replaceFields(fpFieldNums, fm);
                                        }
                                        public void fetchNonLoadedFields(ObjectProvider op)
                                        {
                                            op.replaceNonLoadedFields(fpFieldNums, fm);
                                        }
                                        public FetchPlan getFetchPlanForLoading()
                                        {
                                            return null;
                                        }
                                    }, null, ignoreCache, false);
                                }
                                else
                                {
                                    // Nondurable identity
                                    final FetchFieldManager fm = new FetchFieldManager(ec, cmd, worksheet, rowNumber, table);
                                    Object id = new SCOID(cmd.getFullClassName());
                                    return ec.findObject(id, new FieldValues()
                                    {
                                        // ObjectProvider calls the fetchFields method
                                        public void fetchFields(ObjectProvider op)
                                        {
                                            op.replaceFields(fpFieldNums, fm);
                                        }
                                        public void fetchNonLoadedFields(ObjectProvider sm)
                                        {
                                            sm.replaceNonLoadedFields(fpFieldNums, fm);
                                        }
                                        public FetchPlan getFetchPlanForLoading()
                                        {
                                            return null;
                                        }
                                    }, null, ignoreCache, false);
                                }
                            }

                            current++;
                        }
                    }
                }
            }
            else
            {
                first += number;
            }
        }

        return null;
    }
}
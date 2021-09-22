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
2008 Andy Jefferson - app id dups check, row number finder, factor much code into ExcelUtils
 ...
***********************************************************************/
package org.datanucleus.store.excel;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.excel.fieldmanager.FetchFieldManager;
import org.datanucleus.store.excel.fieldmanager.StoreFieldManager;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Persistence Handler for Excel datastores. 
 * Handles the insert/update/delete/fetch/locate operations by using Apache POI.
 * Some notes about Apache POI utilisation :-
 * <ul>
 * <li>We have a Workbook, composed of a set of named Sheet objects.</li>
 * <li>Each class is persisted to its own sheet.</li>
 * <li>Insert of an object requires creation of a Row. Unless we are on the last row
 *     in the sheet in which case we have a row and just need to add cells. See "delete"</li>
 * <li>Delete of an object will involve removal of the row, EXCEPT in the case of the final row
 *     in the sheet in which case we have to delete all cells since Apache POI doesn't seem to
 *     allow removal of the last row.</li>
 * </ul>
 */
public class ExcelPersistenceHandler extends AbstractPersistenceHandler
{
    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public ExcelPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    public void close()
    {
        // Nothing to do since we maintain no resources
    }

    /**
     * Method to insert the object into the datastore.
     * @param op StateManager of the object
     */
    public void insertObject(final ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.Insert.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Workbook wb = (Workbook) mconn.getConnection();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ExcelStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), wb);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Enforce uniqueness of datastore rows
                try
                {
                    locateObject(op);
                    throw new NucleusUserException(Localiser.msg("Excel.Insert.ObjectWithIdAlreadyExists",
                        op.getObjectAsPrintable(), op.getInternalObjectId()));
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // Do nothing since object with this id doesn't exist
                }
            }

            int[] fieldNumbers = cmd.getAllMemberPositions();
            String sheetName = table.getName();
            Sheet sheet = wb.getSheet(sheetName);
            int rowNum = 0;
            if (sheet == null)
            {
                // Sheet doesn't exist so create it
                sheet = wb.createSheet(sheetName);
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.Insert.SheetCreated", op.getObjectAsPrintable(), sheetName));
                }
            }
            else
            {
                // Find number of active rows in this sheet
                rowNum += ExcelUtils.getNumberOfRowsInSheetOfWorkbook(op, wb);
            }

            // Create the object in the datastore
            Row row = sheet.getRow(rowNum);
            if (row == null)
            {
                // No row present so create holder for the cells
                row = sheet.createRow(rowNum);
            }

            op.provideFields(fieldNumbers, new StoreFieldManager(op, row, true, table));

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Set the datastore identity column value
                int idCellNum = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
                Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
                Cell idCell = row.getCell(idCellNum);
                if (idCell == null)
                {
                    idCell = row.createCell(idCellNum);
                }
                if (key instanceof String)
                {
                    idCell.setCellValue(wb.getCreationHelper().createRichTextString((String)key));
                }
                else
                {
                    idCell.setCellValue(((Long)key).longValue());
                }
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // versioned object so set its version
                Cell verCell = null;
                if (vermd.getFieldName() != null)
                {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    MemberColumnMapping mapping = table.getMemberColumnMappingForMember(verMmd);
                    verCell = row.getCell(mapping.getColumn(0).getPosition());
                    if (verCell == null)
                    {
                        verCell = row.createCell(mapping.getColumn(0).getPosition());
                    }
                }
                else
                {
                    int verCellNum = table.getSurrogateColumn(SurrogateColumnType.VERSION).getPosition();
                    verCell = row.getCell(verCellNum);
                    if (verCell == null)
                    {
                        verCell = row.createCell(verCellNum);
                    }
                }

                Object nextVersion = ec.getLockManager().getNextVersion(vermd, null);
                op.setTransactionalVersion(nextVersion);
                if (nextVersion instanceof Long)
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(Localiser.msg("Excel.Insert.ObjectPersistedWithVersion",
                            op.getObjectAsPrintable(), op.getInternalObjectId(), "" + nextVersion));
                    }
                    verCell.setCellValue((Long)nextVersion);
                }
                else if (nextVersion instanceof Timestamp)
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(Localiser.msg("Excel.Insert.ObjectPersistedWithVersion",
                            op.getObjectAsPrintable(), op.getInternalObjectId(), "" + nextVersion));
                    }
                    Date date = new Date();
                    date.setTime(((Timestamp)nextVersion).getTime());
                    verCell.setCellValue(date);
                }
            }
            else
            {
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("Excel.Insert.ObjectPersisted",
                        op.getObjectAsPrintable(), op.getInternalObjectId()));
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method to handle the update of fields of an object in the datastore.
     * @param op StateManager for the object
     * @param fieldNumbers Absolute numbers of fields to be updated
     */
    public void updateObject(final ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            Workbook wb = (Workbook) mconn.getConnection();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ExcelStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), wb);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            final Sheet sheet = ExcelUtils.getSheetForClass(op, wb, table);

            int[] updatedFieldNums = fieldNumbers;
            Object nextVersion = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                Object currentVersion = op.getTransactionalVersion();
                if (currentVersion instanceof Integer)
                {
                    // Cater for Integer-based versions TODO Generalise this
                    currentVersion = Long.valueOf(((Integer)currentVersion).longValue());
                }

                if (cmd.isVersioned())
                {
                    NucleusLogger.PERSISTENCE.warn("This datastore doesn't support optimistic version checks since the datastore file is for a single-connection");
                }

                // Version object so calculate version to store with
                nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                if (vermd.getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    if (verMmd.getType() == Integer.class || verMmd.getType() == int.class)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = Integer.valueOf(((Long)nextVersion).intValue());
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length+1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            // Update the row in the worksheet
            final Row row = sheet.getRow(ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, true, table));
            if (row == null)
            {
                throw new NucleusDataStoreException(Localiser.msg("Excel.RowNotFoundForSheetForWorkbook",
                    table.getName(), StringUtils.toJVMIDString(op.getInternalObjectId())));
            }
            op.provideFields(updatedFieldNums, new StoreFieldManager(op, row, false, table));

            if (vermd != null)
            {
                // Versioned object so set version cell in spreadsheet
                op.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("Excel.Insert.ObjectPersistedWithVersion",
                        op.getObjectAsPrintable(), op.getInternalObjectId(), "" + nextVersion));
                }

                Cell verCell = null;
                if (vermd.getFieldName() != null)
                {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    MemberColumnMapping mapping = table.getMemberColumnMappingForMember(verMmd);
                    verCell = row.getCell(mapping.getColumn(0).getPosition());
                }
                else
                {
                    verCell = row.getCell(table.getSurrogateColumn(SurrogateColumnType.VERSION).getPosition());
                }
                if (nextVersion instanceof Long)
                {
                    verCell.setCellValue(((Long)nextVersion).longValue());
                }
                else if (nextVersion instanceof Timestamp)
                {
                    Date date = new Date();
                    date.setTime(((Timestamp)nextVersion).getTime());
                    verCell.setCellValue(date);
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Deletes a persistent object from the database.
     * @param op The StateManager of the object to be deleted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction for this object
     */
    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (cmd.isVersioned())
            {
                NucleusLogger.PERSISTENCE.warn("This datastore doesn't support optimistic version checks since the datastore file is for a single-connection");
            }

            Workbook wb = (Workbook) mconn.getConnection();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ExcelStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), wb);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            final Sheet sheet = ExcelUtils.getSheetForClass(op, wb, table);

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

            // Delete this object
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.Delete.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            int rowId = ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, false, table);
            if (rowId < 0)
            {
                throw new NucleusObjectNotFoundException("Object not found for id " + IdentityUtils.getPersistableIdentityForId(op.getInternalObjectId()), op.getObject());
            }

            if (storeMgr instanceof XLSStoreManager && sheet.getLastRowNum() == rowId)
            {
                // Deleting top row which is last row so just remove all cells and leave row
                // otherwise Apache POI throws an ArrayIndexOutOfBoundsException
                Row row = sheet.getRow(rowId);
                Iterator<Cell> it = row.cellIterator();
                while (it.hasNext())
                {
                    row.removeCell(it.next());
                }
            }
            else
            {
                // Deleting top row so remove it
                sheet.removeRow(sheet.getRow(rowId));
                if (sheet.getLastRowNum()>rowId)
                {
                    sheet.shiftRows(rowId+1, sheet.getLastRowNum(),-1);
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Excel.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Fetches fields of a persistent object from the database.
     * @param op The ObjectProvider of the object to be fetched.
     * @param fieldNumbers The numbers of the fields to be fetched.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     */
    public void fetchObject(final ObjectProvider op, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuilder str = new StringBuilder("Fetching object \"");
            str.append(op.getObjectAsPrintable()).append("\" (id=");
            str.append(op.getInternalObjectId()).append(")").append(" fields [");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                if (i > 0)
                {
                    str.append(",");
                }
                str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
            }
            str.append("]");
            NucleusLogger.PERSISTENCE.debug(str.toString());
        }

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            Workbook wb = (Workbook) mconn.getConnection();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ExcelStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), wb);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            final Sheet sheet = ExcelUtils.getSheetForClass(op, wb, table);

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Excel.Fetch.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            int rowNumber = ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, false, table);
            if (rowNumber < 0)
            {
                throw new NucleusObjectNotFoundException("Object not found for id " + IdentityUtils.getPersistableIdentityForId(op.getInternalObjectId()), op.getObject());
            }
            op.replaceFields(fieldNumbers, new FetchFieldManager(op, sheet, rowNumber, table));

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Excel.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
                ec.getStatistics().incrementFetchCount();
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null && op.getTransactionalVersion() == null)
            {
                // Object has no version set so update it from this fetch
                long verColNo = -1;
                if (vermd.getFieldName() == null)
                {
                    // Surrogate version
                    verColNo = table.getSurrogateColumn(SurrogateColumnType.VERSION).getPosition();
                }
                else
                {
                    // Field-based version
                    verColNo = table.getMemberColumnMappingForMember(cmd.getMetaDataForMember(vermd.getFieldName())).getColumn(0).getPosition();
                }

                Row row = sheet.getRow(rowNumber);
                Cell cell = row.getCell((int)verColNo);
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    op.setVersion(Long.valueOf((long)cell.getNumericCellValue()));
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    op.setVersion(cell.getDateCellValue());
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Accessor for the object with the specified identity (if present).
     * Since we don't manage the memory instantiation of objects this just returns null.
     * @param ec execution context
     * @param id Identity of the object
     * @return The object
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }

    /**
     * Method to locate if an object exists in the datastore.
     * Goes through the rows in the worksheet and finds a row with the required identity.
     * @param op StateManager of object to locate
     */
    public void locateObject(ObjectProvider op)
    {
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            Workbook wb = (Workbook) mconn.getConnection();
            AbstractClassMetaData cmd = op.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ExcelStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), wb);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            int rownum = ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, false, table);
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
            }

            if (rownum >= 0)
            {
                return;
            }
        }
        finally
        {
            mconn.release();
        }

        throw new NucleusObjectNotFoundException("Object not found",op.getInternalObjectId());
    }
}
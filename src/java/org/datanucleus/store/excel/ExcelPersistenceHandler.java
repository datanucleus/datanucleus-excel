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
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.excel.fieldmanager.FetchFieldManager;
import org.datanucleus.store.excel.fieldmanager.StoreFieldManager;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
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
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_EXCEL = Localiser.getInstance(
        "org.datanucleus.store.excel.Localisation", ExcelStoreManager.class.getClassLoader());

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
     * @param op ObjectProvider of the object
     */
    public void insertObject(final ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            // Enforce uniqueness of datastore rows
            try
            {
                locateObject(op);
                throw new NucleusUserException(LOCALISER_EXCEL.msg("Excel.Insert.ObjectWithIdAlreadyExists",
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }
            catch (NucleusObjectNotFoundException onfe)
            {
                // Do nothing since object with this id doesn't exist
            }
        }

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.Insert.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            Workbook wb = (Workbook) mconn.getConnection();
            int[] fieldNumbers = cmd.getAllMemberPositions();
            String sheetName = storeMgr.getNamingFactory().getTableName(cmd);
            Sheet sheet = wb.getSheet(sheetName);
            int rowNum = 0;
            if (sheet == null)
            {
                // Sheet doesn't exist so create it
                sheet = wb.createSheet(sheetName);
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.Insert.SheetCreated",
                        op.getObjectAsPrintable(), sheetName));
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

            op.provideFields(fieldNumbers, new StoreFieldManager(op, row, true));

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Set the datastore identity column value
                int idCellNum = (int)ExcelUtils.getColumnIndexForFieldOfClass(cmd, -1);
                Object key = ((OID)op.getInternalObjectId()).getKeyValue();
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
                int verCellNum = (int)ExcelUtils.getColumnIndexForFieldOfClass(cmd, -2);
                Cell verCell = row.getCell(verCellNum);
                if (verCell == null)
                {
                    verCell = row.createCell(verCellNum);
                }

                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), null);
                op.setTransactionalVersion(nextVersion);
                if (nextVersion instanceof Long)
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER_EXCEL.msg("Excel.Insert.ObjectPersistedWithVersion",
                            op.getObjectAsPrintable(), op.getInternalObjectId(), "" + nextVersion));
                    }
                    verCell.setCellValue((Long)nextVersion);
                }
                else if (nextVersion instanceof Timestamp)
                {
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER_EXCEL.msg("Excel.Insert.ObjectPersistedWithVersion",
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
                    NucleusLogger.DATASTORE.debug(LOCALISER_EXCEL.msg("Excel.Insert.ObjectPersisted",
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
     * @param op Object Provider for the object
     * @param fieldNumbers Absolute numbers of fields to be updated
     */
    public void updateObject(final ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            Workbook wb = (Workbook) mconn.getConnection();
            final Sheet sheet = ExcelUtils.getSheetForClass(op, wb);

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
                nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
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
                StringBuffer fieldStr = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.Update.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            // Update the row in the worksheet
            final Row row = sheet.getRow(ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, true));
            if (row == null)
            {
                String sheetName = storeMgr.getNamingFactory().getTableName(cmd);
                throw new NucleusDataStoreException(LOCALISER_EXCEL.msg("Excel.RowNotFoundForSheetForWorkbook",
                    sheetName, StringUtils.toJVMIDString(op.getInternalObjectId())));
            }
            op.provideFields(updatedFieldNums, new StoreFieldManager(op, row, false));

            if (vermd != null)
            {
                // Versioned object so set version cell in spreadsheet
                op.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(LOCALISER_EXCEL.msg("Excel.Insert.ObjectPersistedWithVersion",
                        op.getObjectAsPrintable(), op.getInternalObjectId(), "" + nextVersion));
                }

                int verCellNum = (int)ExcelUtils.getColumnIndexForFieldOfClass(cmd, -2);
                Cell verCell = row.getCell(verCellNum);
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
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
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
     * @param op The Object Provider of the object to be deleted.
     * @throws NucleusDataStoreException when an error occurs in the datastore communication
     * @throws NucleusOptimisticException thrown if version checking fails on an optimistic transaction for this object
     */
    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            Workbook wb = (Workbook) mconn.getConnection();
            final Sheet sheet = ExcelUtils.getSheetForClass(op, wb);

            if (cmd.isVersioned())
            {
                NucleusLogger.PERSISTENCE.warn("This datastore doesn't support optimistic version checks since the datastore file is for a single-connection");
            }

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

            // Delete this object
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.Delete.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            int rowId = ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, false);
            if (rowId < 0)
            {
                throw new NucleusObjectNotFoundException("object not found", op.getObject());
            }
            else
            {
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
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_EXCEL.msg("Excel.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
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
        AbstractClassMetaData acmd = op.getClassMetaData();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuffer str = new StringBuffer("Fetching object \"");
            str.append(op.getObjectAsPrintable()).append("\" (id=");
            str.append(op.getInternalObjectId()).append(")").append(" fields [");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                if (i > 0)
                {
                    str.append(",");
                }
                str.append(acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
            }
            str.append("]");
            NucleusLogger.PERSISTENCE.debug(str.toString());
        }

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Workbook wb = (Workbook) mconn.getConnection();
            final Sheet sheet = ExcelUtils.getSheetForClass(op, wb);

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_EXCEL.msg("Excel.Fetch.Start", 
                    op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            int rowNumber = ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, false);
            if (rowNumber < 0)
            {
                throw new NucleusObjectNotFoundException("object not found", op.getObject());
            }
            op.replaceFields(fieldNumbers, new FetchFieldManager(op, sheet, rowNumber));

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_EXCEL.msg("Excel.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
                ec.getStatistics().incrementFetchCount();
            }

            VersionMetaData vermd = acmd.getVersionMetaDataForClass();
            if (vermd != null && op.getTransactionalVersion() == null)
            {
                // Object has no version set so update it from this fetch
                long verColNo = -1;
                if (vermd.getFieldName() == null)
                {
                    // Surrogate version
                    verColNo = ExcelUtils.getColumnIndexForFieldOfClass(acmd, -2);
                }
                else
                {
                    // Field-based version
                    verColNo = ExcelUtils.getColumnIndexForFieldOfClass(acmd, 
                        acmd.getAbsolutePositionOfMember(vermd.getFieldName()));
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
     * @param op ObjectProvider of object to locate
     */
    public void locateObject(ObjectProvider op)
    {
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            Workbook wb = (Workbook) mconn.getConnection();
            int rownum = ExcelUtils.getRowNumberForObjectInWorkbook(op, wb, false);
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
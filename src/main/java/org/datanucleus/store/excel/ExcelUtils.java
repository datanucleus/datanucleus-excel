/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;

/**
 * Class providing convenience methods for handling Excel datastores.
 * Please refer to Apache POI http://poi.apache.org
 */
public class ExcelUtils
{
    private ExcelUtils() {}

    /**
     * Convenience method to return the worksheet used for storing the specified object.
     * @param sm StateManager for the object
     * @param wb Workbook
     * @param table The table representing this worksheet
     * @return The Work Sheet
     * @throws NucleusDataStoreException if the work sheet doesn't exist in this workbook
     */
    public static Sheet getSheetForClass(DNStateManager sm, Workbook wb, Table table)
    {
        String sheetName = table.getName();
        final Sheet sheet = wb.getSheet(sheetName);
        if (sheet == null)
        {
            throw new NucleusDataStoreException(Localiser.msg("Excel.SheetNotFoundForWorkbook", sheetName, sm.getObjectAsPrintable()));
        }
        return sheet;
    }

    /**
     * Convenience method to find the row number of an object in the provided workbook.
     * For application-identity does a search for a row with the specified PK field values.
     * For datastore-identity does a search for the row with the datastore column having the specified value
     * @param sm StateManager for the object
     * @param wb Workbook
     * @param originalValue Use the original value of the identifiying fields if available (for when we are updating and using nondurable identity).
     * @param table The table representing this worksheet
     * @return The row number (or -1 if not found)
     */
    public static int getRowNumberForObjectInWorkbook(DNStateManager sm, Workbook wb, boolean originalValue, Table table)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int[] pkFieldNumbers = cmd.getPKMemberPositions();

            List<Integer> pkFieldColList = new ArrayList(pkFieldNumbers.length);
            List pkFieldValList = new ArrayList(pkFieldNumbers.length);
            List<Class> pkFieldTypeList = new ArrayList(pkFieldNumbers.length);
            for (int i=0;i<pkFieldNumbers.length;i++)
            {
                Object fieldValue = sm.provideField(pkFieldNumbers[i]);
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(clr);
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    DNStateManager embSM = ec.findStateManager(fieldValue);
                    if (embSM == null)
                    {
                        embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, fieldValue, false, sm, pkFieldNumbers[i], PersistableObjectType.EMBEDDED_PC);
                    }
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        // TODO Support nested embedded
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(j);
                        List<AbstractMemberMetaData> embMmds = new ArrayList();
                        embMmds.add(mmd);
                        embMmds.add(embMmd);
                        MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(embMmds);
                        pkFieldColList.add(mapping.getColumn(0).getPosition());
                        if (mapping.getTypeConverter() != null)
                        {
                            pkFieldValList.add(mapping.getTypeConverter().toDatastoreType(embSM.provideField(j)));
                            pkFieldTypeList.add(ec.getTypeManager().getDatastoreTypeForTypeConverter(mapping.getTypeConverter(), embMmd.getType()));
                        }
                        else
                        {
                            pkFieldValList.add(embSM.provideField(j));
                            pkFieldTypeList.add(embMmd.getType());
                        }
                    }
                }
                else
                {
                    MemberColumnMapping mapping = table.getMemberColumnMappingForMember(mmd);
                    pkFieldColList.add(mapping.getColumn(0).getPosition());
                    if (mapping.getTypeConverter() != null)
                    {
                        pkFieldValList.add(mapping.getTypeConverter().toDatastoreType(fieldValue));
                        pkFieldTypeList.add(ec.getTypeManager().getDatastoreTypeForTypeConverter(mapping.getTypeConverter(), mmd.getType()));
                    }
                    else
                    {
                        pkFieldValList.add(fieldValue);
                        pkFieldTypeList.add(mmd.getType());
                    }
                }
            }

            String sheetName = table.getName();
            final Sheet sheet = wb.getSheet(sheetName);
            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row row = sheet.getRow(i);
                    if (row != null)
                    {
                        boolean matches = true;

                        for (int j=0;j<pkFieldColList.size();j++)
                        {
                            int colNumber = pkFieldColList.get(j);
                            Object fieldValue = pkFieldValList.get(j);
                            Class fieldType = pkFieldTypeList.get(j);
                            Cell cell = row.getCell(colNumber);
                            if (!cellMatches(cell, fieldType, fieldValue))
                            {
                                matches = false;
                                break;
                            }
                        }
                        if (matches)
                        {
                            // Found the object with the correct PK values so return
                            return row.getRowNum();
                        }
                    }
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            String sheetName = table.getName();
            final Sheet sheet = wb.getSheet(sheetName);
            int datastoreIdColNo = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
            Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
            if (sheet != null)
            {
                for (int i=0; i<sheet.getLastRowNum()+1; i++)
                {
                    Row row = sheet.getRow(i);
                    if (row != null)
                    {
                        Cell cell = row.getCell(datastoreIdColNo);
                        if (cell != null && cellMatches(cell, key.getClass(), key))
                        {
                            return row.getRowNum();
                        }
                    }
                }
            }
        }
        else
        {
            // Nondurable, so compare all applicable fields
            ExecutionContext ec = sm.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int[] fieldNumbers = cmd.getAllMemberPositions();

            List<Integer> fieldColList = new ArrayList(fieldNumbers.length);
            List<Class> fieldTypeList = new ArrayList(fieldNumbers.length);
            List fieldValList = new ArrayList(fieldNumbers.length);
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(clr);
                Object fieldValue = null;
                if (originalValue)
                {
                    Object oldValue = sm.getAssociatedValue(DNStateManager.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumbers[i]);
                    if (oldValue != null)
                    {
                        fieldValue = oldValue;
                    }
                    else
                    {
                        fieldValue = sm.provideField(fieldNumbers[i]);
                    }
                }
                else
                {
                    fieldValue = sm.provideField(fieldNumbers[i]);
                }
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    DNStateManager embSM = ec.findStateManager(fieldValue);
                    if (embSM == null)
                    {
                        embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, fieldValue, false, sm, fieldNumbers[i], PersistableObjectType.EMBEDDED_PC);
                    }
                    AbstractClassMetaData embCmd = sm.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        // TODO Support nested embedded
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(j);
                        List<AbstractMemberMetaData> embMmds = new ArrayList();
                        embMmds.add(mmd);
                        embMmds.add(embMmd);
                        fieldColList.add(table.getMemberColumnMappingForEmbeddedMember(embMmds).getColumn(0).getPosition());
                        fieldTypeList.add(embMmd.getType());
                        fieldValList.add(embSM.provideField(j));
                    }
                }
                else if (relationType == RelationType.NONE)
                {
                    fieldColList.add(table.getMemberColumnMappingForMember(mmd).getColumn(0).getPosition());
                    fieldTypeList.add(mmd.getType());
                    fieldValList.add(fieldValue);
                }
            }

            String sheetName = table.getName();
            final Sheet sheet = wb.getSheet(sheetName);
            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row row = sheet.getRow(i);
                    if (row != null)
                    {
                        boolean matches = true;

                        for (int j=0;j<fieldColList.size();j++)
                        {
                            int colNumber = fieldColList.get(j);
                            Class fieldType = fieldTypeList.get(j);
                            Object fieldValue = fieldValList.get(j);

                            Cell cell = row.getCell(colNumber);
                            if (!cellMatches(cell, fieldType, fieldValue))
                            {
                                matches = false;
                                break;
                            }
                        }
                        if (matches)
                        {
                            // Found the object with the correct PK values so return
                            return row.getRowNum();
                        }
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Convenience method to check if a cell value matches the provided value and type.
     * @param cell The cell
     * @param fieldType The type to compare it with
     * @param fieldValue The value of the type to compare it with (can be null)
     * @return Whether the cell matches
     */
    protected static boolean cellMatches(Cell cell, Class fieldType, Object fieldValue)
    {
        if (cell == null)
        {
            return false;
        }

        if (String.class.isAssignableFrom(fieldType) && cell.getRichStringCellValue().getString().equals(fieldValue))
        {
            return true;
        }
        else if ((fieldType == int.class || fieldType == Integer.class) && ((Integer)fieldValue).intValue() == (int)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == long.class || fieldType == Long.class) && ((Long)fieldValue).longValue() == (long)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == short.class || fieldType == Short.class) && ((Short)fieldValue).shortValue() == (short)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == float.class || fieldType == Float.class) && ((Float)fieldValue).floatValue() == (float)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == double.class || fieldType == Double.class) && ((Double)fieldValue).doubleValue() == cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == boolean.class || fieldType == Boolean.class) && ((Boolean)fieldValue).booleanValue() == cell.getBooleanCellValue())
        {
            return true;
        }
        else if ((fieldType == byte.class || fieldType == Byte.class) && ((Byte)fieldValue).byteValue() == (byte)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == char.class || fieldType == Character.class) && ((Character)fieldValue).charValue() == cell.getRichStringCellValue().getString().charAt(0))
        {
            return true;
        }
        else if ((Date.class.isAssignableFrom(fieldType) && ((Date)fieldValue).getTime() == cell.getDateCellValue().getTime()))
        {
            return true;
        }
        return false;
    }

    /**
     * Convenience method to find the number of rows in a workbook.
     * This takes into account the fact that it seems to be impossible (with Apache POI 3.0.2)
     * to delete rows from a sheet. Consequently what we do is leave the row but delete
     * all cells. When returning the number of rows this ignores rows that have no cells.
     * @param sm StateManager for the object
     * @param wb Workbook
     * @return Number of (active) rows (or 0 if no active rows)
     */
    public static int getNumberOfRowsInSheetOfWorkbook(DNStateManager sm, Workbook wb)
    {
        int numRows = 0;

        final AbstractClassMetaData cmd = sm.getClassMetaData();
        Table table = sm.getExecutionContext().getStoreManager().getStoreDataForClass(sm.getClassMetaData().getFullClassName()).getTable();
        String sheetName = table.getName();
        final Sheet sheet = wb.getSheet(sheetName);
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            int[] pkFieldNumbers = cmd.getPKMemberPositions();
            Object[] pkFieldValues = new Object[pkFieldNumbers.length];
            for (int i=0;i<pkFieldNumbers.length;i++)
            {
                pkFieldValues[i] = sm.provideField(pkFieldNumbers[i]);
            }

            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row row = sheet.getRow(i);
                    if (row != null)
                    {
                        boolean validRow = true;
                        for (int j=0;j<pkFieldNumbers.length;j++)
                        {
                            AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[j]);
                            int colNumber = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getPosition();
                            Cell cell = row.getCell(colNumber);
                            if (cell == null)
                            {
                                // Valid row. Apache POI would return cell as null if not active
                                validRow = false;
                            }
                        }
                        if (validRow)
                        {
                            numRows++;
                        }
                    }
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                int datastoreIdColNumber = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row rrow = sheet.getRow(i);
                    Cell cell = rrow.getCell(datastoreIdColNumber);
                    if (cell != null)
                    {
                        // Valid row. Apache POI would return cell as null if not active
                        numRows++;
                    }
                }
            }
        }
        else
        {
            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row rrow = sheet.getRow(i);
                    Cell cell = rrow.getCell(0); // Use first cell since no identity as such
                    if (cell != null)
                    {
                        // Valid row. Apache POI would return cell as null if not active
                        numRows++;
                    }
                }
            }
        }

        return numRows;
    }
}
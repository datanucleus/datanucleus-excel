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
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.schema.naming.NamingFactory;
import org.datanucleus.util.Localiser;

/**
 * Class providing convenience methods for handling Excel datastores.
 * Please refer to Apache POI http://poi.apache.org
 */
public class ExcelUtils
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.excel.Localisation", ExcelStoreManager.class.getClassLoader());

    /**
     * Convenience method to get the index number where a field of a class is persisted.
     * Uses column "position" expecting an integer value.
     * The input field number is the absolute number (0 or higher); a value of -1 implies surrogate identity column,
     * and -2 implies surrogate version column
     * @param acmd MetaData for the class
     * @param fieldNumber Absolute field number that we are interested in.
     */
    public static int getColumnIndexForFieldOfClass(AbstractClassMetaData acmd, int fieldNumber)
    {
        if (fieldNumber >= 0)
        {
            // Field of the class
            AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            Integer colPos = (ammd.getColumnMetaData() == null || ammd.getColumnMetaData().length == 0 ?
                    null : ammd.getColumnMetaData()[0].getPosition());
            if (colPos == null)
            {
                ColumnMetaData[] colmds = ammd.getColumnMetaData();
                if (colmds != null && colmds.length > 0)
                {
                    String colName = colmds[0].getName();
                    try
                    {
                        return Integer.valueOf(colName).intValue();
                    }
                    catch (NumberFormatException nfe)
                    {
                        return (int)fieldNumber;
                    }
                }
                else
                {
                    return (int)fieldNumber;
                }
            }
            else
            {
                return colPos.intValue();
            }
        }
        else if (fieldNumber == -1)
        {
            // Surrogate datastore identity column
            IdentityMetaData imd = acmd.getIdentityMetaData();
            if (imd != null)
            {
                Integer colPos = (imd.getColumnMetaData() == null ? null : imd.getColumnMetaData().getPosition());
                if (colPos == null)
                {
                    if (imd.getColumnMetaData() != null)
                    {
                        String colName = imd.getColumnMetaData().getName();
                        try
                        {
                            return Integer.valueOf(colName).intValue();
                        }
                        catch (NumberFormatException nfe)
                        {
                        }
                    }
                }
                else
                {
                    colPos.intValue();
                }
            }
            int fallbackPosition = acmd.getNoOfInheritedManagedMembers() + acmd.getNoOfManagedMembers();
            return fallbackPosition;
        }
        else if (fieldNumber == -2)
        {
            // Surrogate version column
            VersionMetaData vmd = acmd.getVersionMetaDataForClass();
            if (vmd != null)
            {
                Integer colPos = (vmd.getColumnMetaData() == null ? null : vmd.getColumnMetaData().getPosition());
                if (colPos == null)
                {
                    if (vmd.getColumnMetaData() != null)
                    {
                        String colName = vmd.getColumnMetaData().getName();
                        try
                        {
                            return Integer.valueOf(colName).intValue();
                        }
                        catch (NumberFormatException nfe)
                        {
                        }
                    }
                }
                else
                {
                    colPos.intValue();
                }
            }
            int fallbackPosition = acmd.getNoOfInheritedManagedMembers() + acmd.getNoOfManagedMembers() + 1;
            return fallbackPosition;
        }
        else
        {
            throw new NucleusException("Unsupported field number " + fieldNumber);
        }
    }

    public static int getColumnIndexForFieldOfEmbeddedClass(AbstractClassMetaData acmd, int fieldNumber, AbstractMemberMetaData mmd)
    {
        if (fieldNumber >= 0)
        {
        	EmbeddedMetaData emd = mmd.getEmbeddedMetaData();
        	AbstractMemberMetaData[] emb_mmd = emd.getMemberMetaData();
            // Field of the class
            AbstractMemberMetaData ammd = emb_mmd[fieldNumber];
            Integer colPos = (ammd.getColumnMetaData() == null || ammd.getColumnMetaData().length == 0 ?
                    null : ammd.getColumnMetaData()[0].getPosition());
            if (colPos == null)
            {
                ColumnMetaData[] colmds = ammd.getColumnMetaData();
                if (colmds != null && colmds.length > 0)
                {
                    String colName = colmds[0].getName();
                    try
                    {
                        return Integer.valueOf(colName).intValue();
                    }
                    catch (NumberFormatException nfe)
                    {
                        return fieldNumber;
                    }
                }
                else
                {
                    return fieldNumber;
                }
            }
            else
            {
                return colPos;
            }
        }
        else
        {
            throw new NucleusException("Unsupported field number " + fieldNumber);
        }
    }

    /**
     * Convenience method to return the worksheet used for storing the specified object.
     * @param op ObjectProvider for the object
     * @param wb Workbook
     * @return The Work Sheet
     * @throws NucleusDataStoreException if the work sheet doesn't exist in this workbook
     */
    public static Sheet getSheetForClass(ObjectProvider op, Workbook wb)
    {
        String sheetName = op.getExecutionContext().getStoreManager().getNamingFactory().getTableName(op.getClassMetaData());
        final Sheet sheet = wb.getSheet(sheetName);
        if (sheet == null)
        {
            throw new NucleusDataStoreException(LOCALISER.msg("Excel.SheetNotFoundForWorkbook",
                sheetName, op.getObjectAsPrintable()));
        }
        return sheet;
    }

    /**
     * Convenience method to find the row number of an object in the provided workbook.
     * For application-identity does a search for a row with the specified PK field values.
     * For datastore-identity does a search for the row with the datastore column having the specified value
     * @param op ObjectProvider for the object
     * @param wb Workbook
     * @param originalValue Use the original value of the identifiying fields if available (for when we are updating
     *     and using nondurable identity).
     * @return The row number (or -1 if not found)
     */
    public static int getRowNumberForObjectInWorkbook(ObjectProvider op, Workbook wb, boolean originalValue)
    {
        final AbstractClassMetaData cmd = op.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            ExecutionContext ec = op.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int[] pkFieldNumbers = cmd.getPKMemberPositions();

            List<Integer> pkFieldColList = new ArrayList(pkFieldNumbers.length);
            List pkFieldValList = new ArrayList(pkFieldNumbers.length);
            List<Class> pkFieldTypeList = new ArrayList(pkFieldNumbers.length);
            for (int i=0;i<pkFieldNumbers.length;i++)
            {
                Object fieldValue = op.provideField(pkFieldNumbers[i]);
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(clr);
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    ObjectProvider embOP = ec.findObjectProvider(fieldValue);
                    if (embOP == null)
                    {
                        embOP = ec.newObjectProviderForEmbedded(fieldValue, false, op, pkFieldNumbers[i]);
                    }
                    AbstractClassMetaData embCmd = op.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(j);
                        pkFieldColList.add(getColumnIndexForFieldOfEmbeddedClass(cmd, j, mmd));
                        pkFieldValList.add(embOP.provideField(j));
                        pkFieldTypeList.add(embMmd.getType());
                    }
                }
                else
                {
                    pkFieldColList.add(getColumnIndexForFieldOfClass(cmd, pkFieldNumbers[i]));
                    pkFieldValList.add(fieldValue);
                    pkFieldTypeList.add(mmd.getType());
                }
            }

            String sheetName = op.getExecutionContext().getStoreManager().getNamingFactory().getTableName(cmd);
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
            String sheetName = op.getExecutionContext().getStoreManager().getNamingFactory().getTableName(cmd);
            final Sheet sheet = wb.getSheet(sheetName);
            int datastoreIdColNumber = (int)ExcelUtils.getColumnIndexForFieldOfClass(cmd, -1);
            Object key = ((OID)op.getInternalObjectId()).getKeyValue();
            if (sheet != null)
            {
                for (int i=0; i<sheet.getLastRowNum()+1; i++)
                {
                    Row row = sheet.getRow(i);
                    if (row != null)
                    {
                        Cell cell = row.getCell(datastoreIdColNumber);
                        if (cell != null)
                        {
                            if (cellMatches(cell, key.getClass(), key))
                            {
                                return row.getRowNum();
                            }
                        }
                    }
                }
            }
        }
        else
        {
            // Nondurable, so compare all applicable fields
            ExecutionContext ec = op.getExecutionContext();
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
                    Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumbers[i]);
                    if (oldValue != null)
                    {
                        fieldValue = oldValue;
                    }
                    else
                    {
                        fieldValue = op.provideField(fieldNumbers[i]);
                    }
                }
                else
                {
                    fieldValue = op.provideField(fieldNumbers[i]);
                }
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    ObjectProvider embOP = ec.findObjectProvider(fieldValue);
                    if (embOP == null)
                    {
                        embOP = ec.newObjectProviderForEmbedded(fieldValue, false, op, fieldNumbers[i]);
                    }
                    AbstractClassMetaData embCmd = op.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(j);
                        fieldColList.add(getColumnIndexForFieldOfEmbeddedClass(cmd, j, mmd));
                        fieldTypeList.add(embMmd.getType());
                        fieldValList.add(embOP.provideField(j));
                    }
                }
                else if (relationType == RelationType.NONE)
                {
                    fieldColList.add(getColumnIndexForFieldOfClass(cmd, fieldNumbers[i]));
                    fieldTypeList.add(mmd.getType());
                    fieldValList.add(fieldValue);
                }
            }

            String sheetName = op.getExecutionContext().getStoreManager().getNamingFactory().getTableName(cmd);
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

        if (String.class.isAssignableFrom(fieldType) && 
            cell.getRichStringCellValue().getString().equals(fieldValue))
        {
            return true;
        }
        else if ((fieldType == int.class || fieldType == Integer.class) && 
                ((Integer)fieldValue).intValue() == (int)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == long.class || fieldType == Long.class) && 
                ((Long)fieldValue).longValue() == (long)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == short.class || fieldType == Short.class) && 
                ((Short)fieldValue).shortValue() == (short)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == float.class || fieldType == Float.class) &&
                ((Float)fieldValue).floatValue() == (float)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == double.class || fieldType == Double.class) && 
                ((Double)fieldValue).doubleValue() == (double)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == boolean.class || fieldType == Boolean.class) && 
                ((Boolean)fieldValue).booleanValue() == cell.getBooleanCellValue())
        {
            return true;
        }
        else if ((fieldType == byte.class || fieldType == Byte.class) && 
                ((Byte)fieldValue).byteValue() == (byte)cell.getNumericCellValue())
        {
            return true;
        }
        else if ((fieldType == char.class || fieldType == Character.class) && 
                ((Character)fieldValue).charValue() == cell.getRichStringCellValue().getString().charAt(0))
        {
            return true;
        }
        else if ((Date.class.isAssignableFrom(fieldType) && 
                ((Date)fieldValue).getTime() == cell.getDateCellValue().getTime()))
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
     * @param op ObjectProvider for the object
     * @param wb Workbook
     * @return Number of (active) rows (or 0 if no active rows)
     */
    public static int getNumberOfRowsInSheetOfWorkbook(ObjectProvider op, Workbook wb)
    {
        int numRows = 0;

        final AbstractClassMetaData cmd = op.getClassMetaData();
        NamingFactory namingFactory = op.getExecutionContext().getStoreManager().getNamingFactory();
        String sheetName = namingFactory.getTableName(cmd);
        final Sheet sheet = wb.getSheet(sheetName);
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            int[] pkFieldNumbers = cmd.getPKMemberPositions();
            Object[] pkFieldValues = new Object[pkFieldNumbers.length];
            for (int i=0;i<pkFieldNumbers.length;i++)
            {
                pkFieldValues[i] = op.provideField(pkFieldNumbers[i]);
            }

            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row row = sheet.getRow(i);
                    if (row != null)
                    {
                        for (int j=0;j<pkFieldNumbers.length;j++)
                        {
                            int colNumber = (int)ExcelUtils.getColumnIndexForFieldOfClass(cmd, pkFieldNumbers[j]);
                            Cell cell = row.getCell(colNumber);
                            if (cell != null)
                            {
                                // Valid row. Apache POI would return cell as null if not active
                                numRows++;
                            }
                        }
                    }
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            if (sheet != null && sheet.getPhysicalNumberOfRows() > 0)
            {
                int datastoreIdColNumber = (int)ExcelUtils.getColumnIndexForFieldOfClass(cmd, -1);
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
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
2008 Andy Jefferson - change to use ExcelUtils
2008 Andy Jefferson - mechanism for persisting types as String
2011 Andy Jefferson - clean up, and add support for Maps
 ...
***********************************************************************/
package org.datanucleus.store.excel.fieldmanager;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.Base64;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager to handle the store information into an Excel worksheet row using an object.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    protected final Table table;
    protected final Row row;

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Row row, boolean insert, Table table)
    {
        super(ec, cmd, insert);
        this.row = row;
        this.table = table;
    }

    public StoreFieldManager(ObjectProvider op, Row row, boolean insert, Table table)
    {
        super(op, insert);
        this.row = row;
        this.table = table;

        if (!op.isEmbedded())
        {
            // Add PK field(s) cell, so that the row is detected by ExcelUtils.getNumberOfRowsInSheetOfWorkbook
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNumbers = cmd.getPKMemberPositions();
                for (int j=0;j<pkFieldNumbers.length;j++)
                {
                    AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[j]);
                    int colNumber = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getPosition();
                    if (row.getCell(colNumber) == null)
                    {
                        row.createCell(colNumber);
                    }
                }
            }
            else if (op.getClassMetaData().getIdentityType() == IdentityType.DATASTORE)
            {
                int datastoreIdColNumber = table.getDatastoreIdColumn().getPosition();
                if (row.getCell(datastoreIdColNumber) == null)
                {
                    row.createCell(datastoreIdColNumber);
                }
            }
        }
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue((double)value);
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        cell.setCellValue(value);
    }

    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index);
        if (cell == null)
        {
            cell = row.createCell((int)index);
        }
        if (value == null)
        {
            row.removeCell(cell);
        }
        else
        {
            CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
            cell.setCellValue(createHelper.createRichTextString(value));
        }
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        // Special cases
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                int[] embMmdPosns = embCmd.getAllMemberPositions();
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                if (value == null)
                {
                    // Store null in all columns for the embedded (and nested embedded) object(s)
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, row, insert, embMmds, table);
                    for (int i=0;i<embMmdPosns.length;i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Store a null for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            for (int j=0;j<mapping.getNumberOfColumns();j++)
                            {
                                // TODO Put null in this column
                            }
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, row, insert, embMmds, table);
                embOP.provideFields(embMmdPosns, storeEmbFM);
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with Excel");
            }
        }

        storeObjectFieldInCell(fieldNumber, value, mmd, clr, relationType);
    }

    protected void storeObjectFieldInCell(int fieldNumber, Object value, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    {
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        if (mapping.getNumberOfColumns() > 1)
        {
            // TODO Support multicolumn mappings
            throw new NucleusException("Dont yet support members being mapped to multiple columns : " + mapping.getMemberMetaData().getFullFieldName());
        }

        int index = getColumnMapping(fieldNumber).getColumn(0).getPosition();
        Cell cell = row.getCell((int)index, Row.CREATE_NULL_AS_BLANK);
        if (value == null)
        {
            row.removeCell(cell);
        }
        else if (relationType == RelationType.NONE)
        {
            if (mapping.getTypeConverter() != null)
            {
                // Persist using the provided converter
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(mapping.getTypeConverter(), mmd.getType());
                if (mapping.getNumberOfColumns() == 1)
                {
                    if (datastoreType == String.class)
                    {
                        CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
                        cell.setCellValue(createHelper.createRichTextString((String)datastoreValue));
                        return;
                    }
                    else if (Number.class.isAssignableFrom(datastoreType))
                    {
                        cell.setCellValue((Double)datastoreValue);
                        return;
                    }
                    else if (Boolean.class.isAssignableFrom(datastoreType))
                    {
                        cell.setCellValue((Boolean)datastoreValue);
                        return;
                    }
                    else if (Date.class.isAssignableFrom(datastoreType))
                    {
                        cell.setCellValue((Date)datastoreValue);
                        return;
                    }
                    else
                    {
                        NucleusLogger.DATASTORE_PERSIST.warn("TypeConverter for member " + mmd.getFullFieldName() + " converts to " + datastoreType.getName() + " - not yet supported");
                    }
                }
                else
                {
                    // TODO Support multicolumn conversion
                }
            }
            else if (Number.class.isAssignableFrom(mmd.getType()))
            {
                cell.setCellValue(((Number)value).doubleValue());
            }
            else if (Character.class.isAssignableFrom(mmd.getType()))
            {
                cell.setCellValue(row.getSheet().getWorkbook().getCreationHelper().createRichTextString("" + value));
            }
            else if (Boolean.class.isAssignableFrom(mmd.getType()))
            {
                cell.setCellValue(((Boolean)value).booleanValue());
            }
            else if (Date.class.isAssignableFrom(mmd.getType()))
            {
                cell.setCellValue((Date)value);
            }
            else if (Calendar.class.isAssignableFrom(mmd.getType()))
            {
                cell.setCellValue((Calendar)value);
            }
            else if (Enum.class.isAssignableFrom(mmd.getType()))
            {
                if (MetaDataUtils.isJdbcTypeNumeric(mapping.getColumn(0).getJdbcType()))
                {
                    cell.setCellValue(((Enum)value).ordinal());
                }
                else
                {
                    CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
                    cell.setCellValue(createHelper.createRichTextString(((Enum)value).name()));
                }
            }
            else if (byte[].class == mmd.getType())
            {
                String strValue = new String(Base64.encode((byte[]) value));
                cell.setCellValue(strValue);
            }
            else
            {
                // Try to persist using converters
                TypeManager typeMgr = ec.getNucleusContext().getTypeManager();
                boolean useLong = MetaDataUtils.isJdbcTypeNumeric(mapping.getColumn(0).getJdbcType());

                TypeConverter longConv = typeMgr.getTypeConverterForType(mmd.getType(), Long.class);
                if (useLong)
                {
                    if (longConv != null)
                    {
                        cell.setCellValue((Long)longConv.toDatastoreType(value));
                        return;
                    }
                }
                else
                {
                    TypeConverter strConv = typeMgr.getTypeConverterForType(mmd.getType(), String.class);
                    if (strConv != null)
                    {
                        CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
                        cell.setCellValue(createHelper.createRichTextString((String) strConv.toDatastoreType(value)));
                        return;
                    }
                    else if (longConv != null)
                    {
                        cell.setCellValue((Long)longConv.toDatastoreType(value));
                        return;
                    }
                }

                NucleusLogger.PERSISTENCE.warn(
                    "DataNucleus doesnt currently support persistence of field " + mmd.getFullFieldName() + 
                    " type=" + value.getClass().getName() + " - ignoring");
            }
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object - persist the related object and store the identity in the cell
            Object valuePC = ec.persistObjectInternal(value, op, fieldNumber, -1);
            Object valueId = ec.getApiAdapter().getIdForObject(valuePC);
            CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
            cell.setCellValue(createHelper.createRichTextString("[" + IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valueId) + "]"));
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            if (mmd.hasCollection())
            {
                StringBuffer cellValue = new StringBuffer("[");
                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                    cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
                    if (collIter.hasNext())
                    {
                        cellValue.append(",");
                    }
                }
                cellValue.append("]");
                CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
                cell.setCellValue(createHelper.createRichTextString(cellValue.toString()));
            }
            else if (mmd.hasMap())
            {
                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());

                StringBuffer cellValue = new StringBuffer("[");
                Map map = (Map)value;
                Iterator<Map.Entry> mapIter = map.entrySet().iterator();
                while (mapIter.hasNext())
                {
                    Map.Entry entry = mapIter.next();
                    cellValue.append("[");
                    if (keyCmd != null)
                    {
                        Object keyPC = ec.persistObjectInternal(entry.getKey(), op, fieldNumber, -1);
                        Object keyID = ec.getApiAdapter().getIdForObject(keyPC);
                        cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), keyID));
                    }
                    else
                    {
                        cellValue.append(entry.getKey());
                    }
                    cellValue.append("],[");
                    if (valCmd != null)
                    {
                        Object valPC = ec.persistObjectInternal(entry.getValue(), op, fieldNumber, -1);
                        Object valID = ec.getApiAdapter().getIdForObject(valPC);
                        cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valID));
                    }
                    else
                    {
                        cellValue.append(entry.getValue());
                    }
                    cellValue.append("]");
                    if (mapIter.hasNext())
                    {
                        cellValue.append(",");
                    }
                }
                cellValue.append("]");
                CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
                cell.setCellValue(createHelper.createRichTextString(cellValue.toString()));
            }
            else if (mmd.hasArray())
            {
                StringBuffer cellValue = new StringBuffer("[");
                for (int i=0;i<Array.getLength(value);i++)
                {
                    Object element = Array.get(value, i);
                    Object elementPC = ec.persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                    cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
                    if (i < (Array.getLength(value)-1))
                    {
                        cellValue.append(",");
                    }
                }
                cellValue.append("]");
                CreationHelper createHelper = row.getSheet().getWorkbook().getCreationHelper();
                cell.setCellValue(createHelper.createRichTextString(cellValue.toString()));
            }
        }
    }
}
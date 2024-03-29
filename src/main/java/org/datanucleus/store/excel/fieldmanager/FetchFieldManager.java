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
 ...
***********************************************************************/
package org.datanucleus.store.excel.fieldmanager;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Sheet;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.JdbcType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.EnumConversionHelper;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * FieldManager to handle the retrieval of information from an Excel worksheet row/column into a field of an object.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected Table table;
    protected Sheet sheet;
    protected int rowNumber;

    public FetchFieldManager(DNStateManager sm, Sheet sheet, int row, Table table)
    {
        super(sm);
        this.table = table;
        this.rowNumber = row;
        this.sheet = sheet;
    }

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Sheet sheet, int row, Table table)
    {
        super(ec, cmd);
        this.table = table;
        this.rowNumber = row;
        this.sheet = sheet;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return false;
        }
        return cell.getBooleanCellValue();
    }

    public byte fetchByteField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        return (byte) cell.getNumericCellValue();
    }

    public char fetchCharField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        if (cell.getCellType() == CellType.NUMERIC)
        {
            // In case the char was interpreted as a numeric
            return (char)cell.getNumericCellValue();
        }
        return cell.getRichStringCellValue().getString().charAt(0);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        return cell.getNumericCellValue();
    }

    public float fetchFloatField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        return (float) cell.getNumericCellValue();
    }

    public int fetchIntField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        return (int) cell.getNumericCellValue();
    }

    public long fetchLongField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        return (long) cell.getNumericCellValue();
    }

    public short fetchShortField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return 0;
        }
        return (short) cell.getNumericCellValue();
    }

    public String fetchStringField(int fieldNumber)
    {
        Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        if (cell == null)
        {
            return null;
        }
        return cell.getRichStringCellValue().getString();
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);

        // Special cases
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO Null detection
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embCmd, sm, fieldNumber, PersistableObjectType.EMBEDDED_PC);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embSM, sheet, rowNumber, embMmds, table);
                embSM.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embSM.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with Excel");
            }
        }

        return fetchObjectFieldInternal(mmd, clr, relationType);
    }

    protected Object fetchObjectFieldInternal(AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    {
        MemberColumnMapping mapping = getColumnMapping(mmd.getAbsoluteFieldNumber());

        boolean optional = false;
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }
            optional = true;
        }

        if (relationType == RelationType.NONE)
        {
            if (mapping.getTypeConverter() != null)
            {
                TypeConverter conv = mapping.getTypeConverter();
                if (mapping.getNumberOfColumns() == 1)
                {
                    Cell cell = sheet.getRow(rowNumber).getCell(getColumnMapping(mmd.getAbsoluteFieldNumber()).getColumn(0).getPosition());
                    if (cell == null)
                    {
                        return null;
                    }

                    Object value = null;
                    Class datastoreType = ec.getTypeManager().getDatastoreTypeForTypeConverter(conv, mmd.getType());
                    if (datastoreType == String.class)
                    {
                        String cellValue = cell.getRichStringCellValue().getString();
                        if (!StringUtils.isWhitespace(cellValue))
                        {
                            value = conv.toMemberType(cellValue);
                        }
                    }
                    else if (Number.class.isAssignableFrom(datastoreType))
                    {
                        value = conv.toMemberType(cell.getNumericCellValue());
                    }
                    else if (Boolean.class.isAssignableFrom(datastoreType))
                    {
                        value = conv.toMemberType(cell.getBooleanCellValue());
                    }
                    else if (Date.class.isAssignableFrom(datastoreType))
                    {
                        value = conv.toMemberType(cell.getDateCellValue());
                    }
                    else
                    {
                        NucleusLogger.DATASTORE_PERSIST.warn("TypeConverter for member " + mmd.getFullFieldName() + " converts to " + datastoreType.getName() + " - not yet supported");
                    }

                    if (sm != null)
                    {
                        return SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), value, true);
                    }
                    return value;
                }

                // Member stored in multiple columns and convertable using TypeConverter
                boolean isNull = true;
                Object valuesArr = null;
                Class[] colTypes = ((MultiColumnConverter)conv).getDatastoreColumnTypes();
                if (colTypes[0] == int.class)
                {
                    valuesArr = new int[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == long.class)
                {
                    valuesArr = new long[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == double.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == float.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == String.class)
                {
                    valuesArr = new String[mapping.getNumberOfColumns()];
                }
                // TODO Support other types
                else
                {
                    valuesArr = new Object[mapping.getNumberOfColumns()];
                }

                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    Cell cell = sheet.getRow(rowNumber).getCell(mapping.getColumn(i).getPosition());
                    if (cell == null)
                    {
                        Array.set(valuesArr, i, null);
                    }
                    else
                    {
                        isNull = false;
                        if (colTypes[i] == int.class)
                        {
                            Object cellValue = getValueFromCellOfType(mapping, i, cell, Integer.class);
                            Array.set(valuesArr, i, ((Integer)cellValue).intValue());
                        }
                        else if (colTypes[i] == long.class)
                        {
                            Object cellValue = getValueFromCellOfType(mapping, i, cell, Long.class);
                            Array.set(valuesArr, i, ((Long)cellValue).longValue());
                        }
                        else
                        {
                            Object cellValue = getValueFromCellOfType(mapping, i, cell, colTypes[i]);
                            Array.set(valuesArr, i, cellValue);
                        }
                    }
                }

                if (isNull)
                {
                    return null;
                }

                Object memberValue = conv.toMemberType(valuesArr);
                if (sm != null && memberValue != null)
                {
                    memberValue = SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), memberValue, true);
                }
                return memberValue;
            }

            Cell cell = sheet.getRow(rowNumber).getCell(mapping.getColumn(0).getPosition());
            if (cell == null)
            {
                return optional ? Optional.empty() : null;
            }

            Class type = optional ? clr.classForName(mmd.getCollection().getElementType()) : mmd.getType();
            Object value = getValueFromCellOfType(mapping, 0, cell, type);
            value = optional ? (value != null ? Optional.of(value) : Optional.empty()) : value;

            // Wrap the field if it is SCO
            return (sm != null) ? SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), value, true) : value;
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object stored as String reference of the identity
            Cell cell = sheet.getRow(rowNumber).getCell(mapping.getColumn(0).getPosition());
            if (cell == null)
            {
                return optional ? Optional.empty() : null;
            }

            String idStr = cell.getRichStringCellValue().getString();
            if (idStr == null)
            {
                return optional ? Optional.empty() : null;
            }

            if (idStr.startsWith("[") && idStr.endsWith("]"))
            {
                idStr = idStr.substring(1, idStr.length()-1);
                Object obj = null;
                Class memberType = optional ? clr.classForName(mmd.getCollection().getElementType()) : mmd.getType();
                AbstractClassMetaData memberCmd = ec.getMetaDataManager().getMetaDataForClass(memberType, clr);
                try
                {
                    if (memberCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                    {
                        // Uses persistent identity
                        obj = IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
                    }
                    else
                    {
                        // Uses legacy identity
                        obj = IdentityUtils.getObjectFromIdString(idStr, memberCmd, ec, true);
                    }
                }
                catch (NucleusObjectNotFoundException nfe)
                {
                    NucleusLogger.GENERAL.warn("Object=" + sm + " field=" + mmd.getFullFieldName() + " has id=" + idStr + " but could not instantiate object with that identity");
                    return optional ? Optional.empty() : null;
                }
                return optional ? Optional.of(obj) : obj;
            }

            return optional ? Optional.empty() : null;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            Cell cell = sheet.getRow(rowNumber).getCell(mapping.getColumn(0).getPosition());
            if (cell == null)
            {
                return null;
            }

            String cellStr = cell.getRichStringCellValue().getString();
            if (cellStr == null)
            {
                return null;
            }

            if (cellStr.startsWith("[") && cellStr.endsWith("]"))
            {
                cellStr = cellStr.substring(1, cellStr.length()-1);
                String[] components = MetaDataUtils.getInstance().getValuesForCommaSeparatedAttribute(cellStr);
                if (Collection.class.isAssignableFrom(mmd.getType()))
                {
                    Collection<Object> coll;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    boolean changeDetected = false;
                    if (components != null)
                    {
                        AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(ec.getClassLoaderResolver());
                        for (int i=0;i<components.length;i++)
                        {
                            // TODO handle Collection<interface>
                            Object element = null;
                            try
                            {
                                if (elementCmd.usesSingleFieldIdentityClass() && components[i].indexOf(':') > 0)
                                {
                                    // Uses persistent identity
                                    element = IdentityUtils.getObjectFromPersistableIdentity(components[i], elementCmd, ec);
                                }
                                else
                                {
                                    // Uses legacy identity
                                    element = IdentityUtils.getObjectFromIdString(components[i], elementCmd, ec, true);
                                }
                                coll.add(element);
                            }
                            catch (NucleusObjectNotFoundException nfe)
                            {
                                // Object no longer exists. Deleted by user? so ignore
                                changeDetected = true;
                            }
                        }
                    }

                    if (coll instanceof List && mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null && !mmd.getOrderMetaData().getOrdering().equals("#PK"))
                    {
                        // Reorder the collection as per the ordering clause
                        Collection newColl = QueryUtils.orderCandidates((List)coll, clr.classForName(mmd.getCollection().getElementType()), mmd.getOrderMetaData().getOrdering(), ec, clr);
                        if (newColl.getClass() != coll.getClass())
                        {
                            // Type has changed, so just reuse the input
                            coll.clear();
                            coll.addAll(newColl);
                        }
                    }

                    if (sm != null)
                    {
                        coll = (Collection) SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), coll, true);
                        if (changeDetected)
                        {
                            sm.makeDirty(mmd.getAbsoluteFieldNumber());
                        }
                    }
                    return coll;
                }
                else if (Map.class.isAssignableFrom(mmd.getType()))
                {
                    AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
                    AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr);

                    Map map;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                        map = (Map) instanceType.getDeclaredConstructor().newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    boolean changeDetected = false;
                    if (components != null)
                    {
                        for (int i=0;i<components.length;i++)
                        {
                            String keyCmpt = components[i];
                            i++;
                            String valCmpt = components[i];

                            // Strip square brackets from entry bounds
                            String keyStr = keyCmpt.substring(1, keyCmpt.length()-1);
                            String valStr = valCmpt.substring(1, valCmpt.length()-1);

                            boolean keySet = true;
                            boolean valSet = true;
                            Object key = null;
                            if (keyCmd != null)
                            {
                                try
                                {
                                    // TODO handle Map<interface, ?>
                                    if (keyCmd.usesSingleFieldIdentityClass() && keyStr.indexOf(':') > 0)
                                    {
                                        // Uses persistent identity
                                        key = IdentityUtils.getObjectFromPersistableIdentity(keyStr, keyCmd, ec);
                                    }
                                    else
                                    {
                                        // Uses legacy identity
                                        key = IdentityUtils.getObjectFromIdString(keyStr, keyCmd, ec, true);
                                    }
                                }
                                catch (NucleusObjectNotFoundException nfe)
                                {
                                    // Object no longer exists. Deleted by user? so ignore
                                    changeDetected = true;
                                    keySet = false;
                                }
                            }
                            else
                            {
                                String keyTypeName = mmd.getMap().getKeyType();
                                Class keyType = ec.getClassLoaderResolver().classForName(keyTypeName);
                                if (Enum.class.isAssignableFrom(keyType))
                                {
                                    key = Enum.valueOf(keyType, keyStr);
                                }
                                else if (keyType == String.class)
                                {
                                    key = keyStr;
                                }
                                else
                                {
                                    // TODO Support other map key types
                                    throw new NucleusException("Don't currently support retrieval of Maps with keys of type " + keyTypeName + " (field="+mmd.getFullFieldName() + ")");
                                }
                            }

                            Object val = null;
                            if (valCmd != null)
                            {
                                try
                                {
                                    // TODO handle Collection<?, interface>
                                    if (valCmd.usesSingleFieldIdentityClass() && valStr.indexOf(':') > 0)
                                    {
                                        // Uses persistent identity
                                        val = IdentityUtils.getObjectFromPersistableIdentity(valStr, valCmd, ec);
                                    }
                                    else
                                    {
                                        // Uses legacy identity
                                        val = IdentityUtils.getObjectFromIdString(valStr, valCmd, ec, true);
                                    }
                                }
                                catch (NucleusObjectNotFoundException nfe)
                                {
                                    // Object no longer exists. Deleted by user? so ignore
                                    changeDetected = true;
                                    valSet = false;
                                }
                            }
                            else
                            {
                                String valTypeName = mmd.getMap().getValueType();
                                Class valType = ec.getClassLoaderResolver().classForName(valTypeName);
                                if (Enum.class.isAssignableFrom(valType))
                                {
                                    val = Enum.valueOf(valType, valStr);
                                }
                                else if (valType == String.class)
                                {
                                    val = valStr;
                                }
                                else
                                {
                                    // TODO Support other map value types
                                    throw new NucleusException("Don't currently support retrieval of Maps with values of type " + valTypeName + " (field="+mmd.getFullFieldName() + ")");
                                }
                            }

                            if (keySet && valSet)
                            {
                                map.put(key, val);
                            }
                        }
                    }
                    if (sm != null)
                    {
                        map = (Map) SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), map, true);
                        if (changeDetected)
                        {
                            sm.makeDirty(mmd.getAbsoluteFieldNumber());
                        }
                    }
                    return map;
                }
                else if (mmd.getType().isArray())
                {
                    Object array = null;
                    boolean changeDetected = false;
                    int pos = 0;
                    if (components != null)
                    {
                        AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(ec.getClassLoaderResolver());
                        array = Array.newInstance(mmd.getType().getComponentType(), components.length);
                        for (int i=0;i<components.length;i++)
                        {
                            // TODO handle interface[]
                            Object element = null;
                            try
                            {
                                if (elementCmd.usesSingleFieldIdentityClass() && components[i].indexOf(':') > 0)
                                {
                                    // Uses persistent identity
                                    element = IdentityUtils.getObjectFromPersistableIdentity(components[i], elementCmd, ec);
                                }
                                else
                                {
                                    // Uses legacy identity
                                    element = IdentityUtils.getObjectFromIdString(components[i], elementCmd, ec, true);
                                }
                                Array.set(array, pos++, element);
                            }
                            catch (NucleusObjectNotFoundException nfe)
                            {
                                // Object no longer exists. Deleted by user? so ignore
                                changeDetected = true;
                            }
                        }
                    }
                    else
                    {
                        array = Array.newInstance(mmd.getType().getComponentType(), 0);
                    }

                    if (changeDetected)
                    {
                        if (pos < Array.getLength(array))
                        {
                            // Some elements not found, so resize the array
                            Object arrayOld = array;
                            array = Array.newInstance(mmd.getType().getComponentType(), pos);
                            for (int j = 0; j < pos; j++)
                            {
                                Array.set(array, j, Array.get(arrayOld, j));
                            }
                        }
                        if (sm != null)
                        {
                            array = SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), array, true);
                            if (changeDetected)
                            {
                                sm.makeDirty(mmd.getAbsoluteFieldNumber());
                            }
                        }
                    }
                    return array;
                }
            }
        }
        throw new NucleusException("Dont currently support retrieval of type " + mmd.getTypeName());
    }

    protected Object getValueFromCellOfType(MemberColumnMapping mapping, int pos, Cell cell, Class requiredType)
    {
        AbstractMemberMetaData mmd = mapping.getMemberMetaData();

        if (Date.class.isAssignableFrom(requiredType))
        {
            Date date = cell.getDateCellValue();
            if (date == null)
            {
                return null;
            }

            Object value = date;
            if (requiredType == java.sql.Date.class)
            {
                value = new java.sql.Date(date.getTime());
            }
            else if (requiredType == java.sql.Time.class)
            {
                value = new java.sql.Time(date.getTime());
            }
            else if (requiredType == java.sql.Timestamp.class)
            {
                value = new java.sql.Timestamp(date.getTime());
            }
            return value;
        }
        else if (Calendar.class.isAssignableFrom(requiredType))
        {
            Date date = cell.getDateCellValue();
            if (date == null)
            {
                return null;
            }

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            return cal;
        }
        else if (Boolean.class.isAssignableFrom(requiredType))
        {
            boolean boolValue = cell.getBooleanCellValue();
            return Boolean.valueOf(boolValue);
        }
        else if (String.class.isAssignableFrom(requiredType))
        {
            return cell.getRichStringCellValue().getString();
        }
        else if (Character.class.isAssignableFrom(requiredType))
        {
            String strValue = cell.getRichStringCellValue().getString();
            return Character.valueOf(strValue.charAt(0));
        }
        else if (Number.class.isAssignableFrom(requiredType))
        {
            double val = cell.getNumericCellValue();
            if (Double.class.isAssignableFrom(requiredType))
            {
                return Double.valueOf(val);
            }
            else if (Float.class.isAssignableFrom(requiredType))
            {
                return Float.valueOf((float)val);
            }
            else if (Integer.class.isAssignableFrom(requiredType))
            {
                return Integer.valueOf((int)val);
            }
            else if (Long.class.isAssignableFrom(requiredType))
            {
                return Long.valueOf((long)val);
            }
            else if (Short.class.isAssignableFrom(requiredType))
            {
                return Short.valueOf((short)val);
            }
            else if (Byte.class.isAssignableFrom(requiredType))
            {
                return Byte.valueOf((byte)val);
            }
            else if (BigDecimal.class.isAssignableFrom(requiredType))
            {
                return new BigDecimal(val);
            }
            else if (BigInteger.class.isAssignableFrom(requiredType))
            {
                return new BigInteger("" + (Double.valueOf(val).longValue()));
            }
        }
        else if (Enum.class.isAssignableFrom(requiredType))
        {
            JdbcType enumJdbcType = MetaDataUtils.getJdbcTypeForEnum(mmd, FieldRole.ROLE_FIELD, ec.getClassLoaderResolver());
            Object datastoreValue = null;
            if (MetaDataUtils.isJdbcTypeNumeric(enumJdbcType))
            {
                datastoreValue = cell.getNumericCellValue();
            }
            else
            {
                datastoreValue = cell.getRichStringCellValue().getString();
            }
            if (datastoreValue == null)
            {
                return null;
            }
            return EnumConversionHelper.getEnumForStoredValue(mmd, FieldRole.ROLE_FIELD, datastoreValue, ec.getClassLoaderResolver());
        }
        else if (requiredType == byte[].class)
        {
            String value = cell.getStringCellValue();
            if (value != null)
            {
                return Base64.getDecoder().decode(value);
            }
        }

        // Fallback to String/Long TypeConverters
        if (cell.getCellType() == CellType.NUMERIC)
        {
            TypeConverter longConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(requiredType, Long.class);
            return longConv.toMemberType((long)cell.getNumericCellValue());
        }
        else if (cell.getCellType() == CellType.STRING)
        {
            TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(requiredType, String.class);
            String cellValue = (cell.getRichStringCellValue() != null ? cell.getRichStringCellValue().getString() : null);
            if (cellValue != null && cellValue.length() > 0)
            {
                return strConv.toMemberType(cell.getRichStringCellValue().getString());
            }
        }

        // Not supported as String so just set to null
        NucleusLogger.PERSISTENCE.warn("Field could not be set in the object since it is not persistable to Excel (type=" + requiredType.getName() + ")");
        return null;
    }
}
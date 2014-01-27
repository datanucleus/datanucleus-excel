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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.excel.ExcelUtils;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.Base64;
import org.datanucleus.util.NucleusLogger;

/**
 * FieldManager to handle the retrieval of information from an Excel worksheet row/column into
 * a field of an object.
 */
public class FetchFieldManager extends AbstractFieldManager
{
    protected ObjectProvider op;
    protected ExecutionContext ec;
    protected AbstractClassMetaData cmd;
    protected Sheet sheet;
    protected int row;

    public FetchFieldManager(ObjectProvider op, Sheet sheet, int row)
    {
        this.op = op;
        this.ec = op.getExecutionContext();
        this.cmd = op.getClassMetaData();
        this.row = row;
        this.sheet = sheet;
    }

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Sheet sheet, int row)
    {
        this.op = null;
        this.ec = ec;
        this.cmd = cmd;
        this.row = row;
        this.sheet = sheet;
    }

    protected int getColumnIndexForMember(int memberNumber)
    {
        return ExcelUtils.getColumnIndexForFieldOfClass(cmd, memberNumber);
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return false;
        }
        return cell.getBooleanCellValue();
    }

    public byte fetchByteField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return (byte) cell.getNumericCellValue();
    }

    public char fetchCharField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return cell.getRichStringCellValue().getString().charAt(0);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return cell.getNumericCellValue();
    }

    public float fetchFloatField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return (float) cell.getNumericCellValue();
    }

    public int fetchIntField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return (int) cell.getNumericCellValue();
    }

    public long fetchLongField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return (long) cell.getNumericCellValue();
    }

    public short fetchShortField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return 0;
        }
        return (short) cell.getNumericCellValue();
    }

    public String fetchStringField(int fieldNumber)
    {
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
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
        if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
        {
            // Persistable object embedded into table of this object
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                ObjectProvider embSM = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, sheet, row, mmd);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
        }

        return fetchObjectFieldFromCell(fieldNumber, mmd, clr);
    }

    protected Object fetchObjectFieldFromCell(int fieldNumber, AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        RelationType relationType = mmd.getRelationType(clr);
        int index = getColumnIndexForMember(fieldNumber);
        Row rrow = sheet.getRow(row);
        Cell cell = rrow.getCell(index);
        if (cell == null)
        {
            return null;
        }
        else if (relationType == RelationType.NONE)
        {
            if (mmd.getTypeConverterName() != null)
            {
                // User-defined type converter
                Object value = null;
                TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                Class datastoreType = TypeManager.getDatastoreTypeForTypeConverter(conv, mmd.getType());
                if (datastoreType == String.class)
                {
                    value = conv.toMemberType(cell.getRichStringCellValue().getString());
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

                if (op != null)
                {
                    return op.wrapSCOField(fieldNumber, value, false, false, true);
                }
                return value;
            }
            else if (Date.class.isAssignableFrom(mmd.getType()))
            {
                Date date = cell.getDateCellValue();
                if (date == null)
                {
                    return null;
                }

                Object value = date;
                if (mmd.getType() == java.sql.Date.class)
                {
                    value = new java.sql.Date(date.getTime());
                }
                else if (mmd.getType() == java.sql.Time.class)
                {
                    value = new java.sql.Time(date.getTime());
                }
                else if (mmd.getType() == java.sql.Timestamp.class)
                {
                    value = new java.sql.Timestamp(date.getTime());
                }

                if (op != null)
                {
                    return op.wrapSCOField(fieldNumber, value, false, false, true);
                }
                return value;
            }
            else if (Calendar.class.isAssignableFrom(mmd.getType()))
            {
                Date date = cell.getDateCellValue();
                if (date == null)
                {
                    return null;
                }

                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                if (op != null)
                {
                    return op.wrapSCOField(fieldNumber, cal, false, false, true);
                }
                return cal;
            }
            else if (Boolean.class.isAssignableFrom(mmd.getType()))
            {
                boolean boolValue = cell.getBooleanCellValue();
                return Boolean.valueOf(boolValue);
            }
            else if (Character.class.isAssignableFrom(mmd.getType()))
            {
                String strValue = cell.getRichStringCellValue().getString();
                return Character.valueOf(strValue.charAt(0));
            }
            else if (Number.class.isAssignableFrom(mmd.getType()))
            {
                double val = cell.getNumericCellValue();
                if (Double.class.isAssignableFrom(mmd.getType()))
                {
                    return Double.valueOf(val);
                }
                else if (Float.class.isAssignableFrom(mmd.getType()))
                {
                    return Float.valueOf((float)val);
                }
                else if (Integer.class.isAssignableFrom(mmd.getType()))
                {
                    return Integer.valueOf((int)val);
                }
                else if (Long.class.isAssignableFrom(mmd.getType()))
                {
                    return Long.valueOf((long)val);
                }
                else if (Short.class.isAssignableFrom(mmd.getType()))
                {
                    return Short.valueOf((short)val);
                }
                else if (Byte.class.isAssignableFrom(mmd.getType()))
                {
                    return Byte.valueOf((byte)val);
                }
                else if (BigDecimal.class.isAssignableFrom(mmd.getType()))
                {
                    return new BigDecimal(val);
                }
                else if (BigInteger.class.isAssignableFrom(mmd.getType()))
                {
                    return new BigInteger("" + val);
                }
            }
            else if (Enum.class.isAssignableFrom(mmd.getType()))
            {
                ColumnMetaData colmd = null;
                if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                {
                    colmd = mmd.getColumnMetaData()[0];
                }
                if (MetaDataUtils.persistColumnAsNumeric(colmd))
                {
                    double value = cell.getNumericCellValue();
                    return mmd.getType().getEnumConstants()[(int)value];
                }
                else
                {
                    String value = cell.getRichStringCellValue().getString();
                    if (value != null && value.length() > 0)
                    {
                        return Enum.valueOf(mmd.getType(), value);
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            else if (mmd.getType() == byte[].class)
            {
                String value = cell.getStringCellValue();
                if (value != null)
                {
                    return Base64.decode(value);
                }
            }

            boolean useLong = false;
            ColumnMetaData[] colmds = mmd.getColumnMetaData();
            if (colmds != null && colmds.length == 1)
            {
                String jdbc = colmds[0].getJdbcType();
                if (jdbc != null && (jdbc.equalsIgnoreCase("int") || jdbc.equalsIgnoreCase("integer")))
                {
                    useLong = true;
                }
            }

            // See if we can persist it using built-in converters
            Object value = null;
            TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            TypeConverter longConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
            if (useLong && longConv != null)
            {
                value = longConv.toMemberType((long)cell.getNumericCellValue());
            }
            else if (!useLong && strConv != null)
            {
                String cellValue = (cell.getRichStringCellValue() != null ? cell.getRichStringCellValue().getString() : null);
                if (cellValue != null && cellValue.length() > 0)
                {
                    value = strConv.toMemberType(cell.getRichStringCellValue().getString());
                }
                else
                {
                    return null;
                }
            }
            else if (!useLong && longConv != null)
            {
                value = longConv.toMemberType((long)cell.getNumericCellValue());
            }
            else
            {
                // Not supported as String so just set to null
                NucleusLogger.PERSISTENCE.warn("Field " + mmd.getFullFieldName() + 
                    " could not be set in the object since it is not persistable to Excel");
                return null;
            }

            // Wrap the field if it is SCO
            if (op != null)
            {
                return op.wrapSCOField(fieldNumber, value, false, false, true);
            }
            return value;
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object stored as String reference of the identity
            String idStr = cell.getRichStringCellValue().getString();
            if (idStr == null)
            {
                return null;
            }

            if (idStr.startsWith("[") && idStr.endsWith("]"))
            {
                idStr = idStr.substring(1, idStr.length()-1);
                Object obj = null;
                AbstractClassMetaData memberCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
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
                return obj;
            }
            else
            {
                return null;
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
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
                        coll = (Collection<Object>) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    if (components != null)
                    {
                        for (int i=0;i<components.length;i++)
                        {
                            // TODO handle Collection<interface>
                            AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                                ec.getClassLoaderResolver(), ec.getMetaDataManager());
                            Object element = null;
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
                    }
                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, coll, false, false, true);
                    }
                    return coll;
                }
                else if (Map.class.isAssignableFrom(mmd.getType()))
                {
                    AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                    AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());

                    Map map;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                        map = (Map) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

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

                            Object key = null;
                            if (keyCmd != null)
                            {
                                // TODO handle Map<interface, ?>
                                if (keyCmd.usesSingleFieldIdentityClass() && components[i].indexOf(':') > 0)
                                {
                                    // Uses persistent identity
                                    key = IdentityUtils.getObjectFromPersistableIdentity(components[i], keyCmd, ec);
                                }
                                else
                                {
                                    // Uses legacy identity
                                    key = IdentityUtils.getObjectFromIdString(components[i], keyCmd, ec, true);
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
                                // TODO handle Collection<?, interface>
                                if (valCmd.usesSingleFieldIdentityClass() && components[i].indexOf(':') > 0)
                                {
                                    // Uses persistent identity
                                    val = IdentityUtils.getObjectFromPersistableIdentity(components[i], valCmd, ec);
                                }
                                else
                                {
                                    // Uses legacy identity
                                    val = IdentityUtils.getObjectFromIdString(components[i], valCmd, ec, true);
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

                            map.put(key, val);
                        }
                    }
                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, map, false, false, true);
                    }
                    return map;
                }
                else if (mmd.getType().isArray())
                {
                    Object array = null;
                    if (components != null)
                    {
                        array = Array.newInstance(mmd.getType().getComponentType(), components.length);
                        for (int i=0;i<components.length;i++)
                        {
                            // TODO handle interface[]
                            AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                                ec.getClassLoaderResolver(), ec.getMetaDataManager());
                            Object element = null;
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
                            Array.set(array, i, element);
                        }
                    }
                    else
                    {
                        array = Array.newInstance(mmd.getType().getComponentType(), 0);
                    }
                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, array, false, false, true);
                    }
                    return array;
                }
            }
        }
        throw new NucleusException("Dont currently support retrieval of type " + mmd.getTypeName());
    }
}
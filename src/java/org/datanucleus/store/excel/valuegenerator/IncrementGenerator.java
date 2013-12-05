/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.excel.valuegenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.NucleusLogger;

/**
 * Generator that uses a collection in Excel to store and allocate identity values.
 */
public class IncrementGenerator extends AbstractDatastoreGenerator implements ValueGenerator
{
    static final String INCREMENT_COL_NAME = "increment";

    /** Key used in the Table to access the increment count */
    private String key;

    private String worksheetName = null;

    /**
     * Constructor. Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * @param name Symbolic name for this generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(String name, Properties props)
    {
        super(name, props);
        this.key = properties.getProperty("field-name", name);
        this.worksheetName = properties.getProperty("sequence-table-name");
        if (this.worksheetName == null)
        {
            this.worksheetName = "IncrementTable";
        }
        if (properties.containsKey("key-cache-size"))
        {
            allocationSize = Integer.valueOf(properties.getProperty("key-cache-size"));
        }
        else
        {
            allocationSize = 1;
        }
    }

    public String getName()
    {
        return this.name;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    protected ValueGenerationBlock reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        // Allocate value(s)
        ManagedConnection mconn = connectionProvider.retrieveConnection();
        List oids = new ArrayList();
        try
        {
            // Create the worksheet if not existing
            Workbook spreadsheetDoc = (Workbook)mconn.getConnection();
            Sheet sheet = spreadsheetDoc.getSheet(worksheetName);
            Row row = null;
            Cell valueCell = null;
            if (sheet == null)
            {
                if (!storeMgr.isAutoCreateTables())
                {
                    throw new NucleusUserException(LOCALISER.msg("040011", worksheetName));
                }

                sheet = spreadsheetDoc.createSheet(worksheetName);
                row = sheet.createRow(0);
                Cell cell = row.createCell(0);
                cell.setCellValue(key);
                valueCell = row.createCell(1);
                valueCell.setCellValue(Double.valueOf(0));
            }
            else
            {
                for (int i=sheet.getFirstRowNum(); i<sheet.getLastRowNum()+1; i++)
                {
                    Row tblRow = sheet.getRow(i);
                    if (tblRow != null)
                    {
                        Cell tblCell = tblRow.getCell(0);
                        if (tblCell.getStringCellValue().equals(key))
                        {
                            row = tblRow;
                            valueCell = row.getCell(1);
                            break;
                        }
                    }
                }
                if (row == null)
                {
                    row = sheet.createRow(sheet.getLastRowNum()+1);
                    
                    Cell cell1 = row.createCell(0);
                    cell1.setCellValue(key);
                    valueCell = row.createCell(1);
                    valueCell.setCellValue(Double.valueOf(0));
                }
            }

            // Update the row
            NucleusLogger.VALUEGENERATION.debug("Allowing " + size + " values for increment generator for "+key);
            long currentVal = (long)valueCell.getNumericCellValue();
            valueCell.setCellValue(Double.valueOf(currentVal+size));
            for (int i=0;i<size;i++)
            {
                oids.add(currentVal+1);
                currentVal++;
            }
        }
        finally
        {
            connectionProvider.releaseConnection();
        }
        return new ValueGenerationBlock(oids);
    }
}
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
package org.datanucleus.store.excel.valuegenerator;

import java.util.Properties;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.valuegenerator.AbstractGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.util.StringUtils;

/**
 * Value generator for max(column) of a sheet in the Excel spreadsheet.
 */
public class MaxGenerator extends AbstractGenerator
{
    String sheetName = null;
    int columnIndex = -1;

    /**
     * Constructor.
     * @param name Symbolic name of the generator
     * @param props Any properties controlling its behaviour.
     */
    public MaxGenerator(String name, Properties props)
    {
        super(name, props);
        String val = properties.getProperty("sheet-name");
        if (StringUtils.isWhitespace(val))
        {
            throw new NucleusException("Cannot create \"max\" generator for Excel datastores without sheet-name");
        }
        sheetName = val;

        val = properties.getProperty("column-index");
        if (StringUtils.isWhitespace(val))
        {
            throw new NucleusException("Cannot create \"max\" generator for Excel datastores without column-index");
        }
        try
        {
            columnIndex = Integer.valueOf(val).intValue();
        }
        catch (NumberFormatException nfe)
        {
            throw new NucleusException("Cannot create \"max\" generator for Excel datastores with column-index of " + val);
        }
    }

    /**
     * Method to reserve a block of poids.
     * Only ever reserves a single timestamp, to the time at which it is created.
     * @param size Number of elements to reserve.
     * @return The block.
     */
    protected ValueGenerationBlock reserveBlock(long size)
    {
        // TODO Obtain the current max from the sheet/column
        ValueGenerationBlock block = new ValueGenerationBlock((Object[])null);
        return block;
    }
}
/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
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

import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.ss.usermodel.Workbook;

/**
 * Managed Connection for XLS.
 */
public class XLSManagedConnection extends AbstractExcelManagedConnection
{
    public XLSManagedConnection(String filename)
    {
        super(filename);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.excel.AbstractExcelManagedConnection#getWorkbook()
     */
    @Override
    protected Workbook getWorkbook()
    {
        return new org.apache.poi.hssf.usermodel.HSSFWorkbook();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.excel.AbstractExcelManagedConnection#getWorkbook(java.io.InputStream)
     */
    @Override
    protected Workbook getWorkbook(InputStream is) throws IOException
    {
        return new org.apache.poi.hssf.usermodel.HSSFWorkbook(is);
    }
}
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.transaction.xa.XAResource;

import org.apache.poi.ss.usermodel.Workbook;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.util.NucleusLogger;

/**
 * Managed Connection for XLS or OOXML.
 */
public abstract class AbstractExcelManagedConnection extends AbstractManagedConnection
{
    String filename;

    /** The Excel XLS/OOXML file. */
    File file;
    
    public AbstractExcelManagedConnection(String filename)
    {
        this.filename = filename;
    }

    protected abstract Workbook getWorkbook();

    protected abstract Workbook getWorkbook(InputStream is) throws IOException;

    public Object getConnection()
    {
        if (conn == null)
        {
            try
            {
                file = new File(filename);
                if (!file.exists())
                {
                    // Excel document doesn't exist, so create
                    Workbook wb = getWorkbook();
                    FileOutputStream fileOut = new FileOutputStream(file);
                    wb.write(fileOut);
                    fileOut.close();
                }

                conn = getWorkbook(new FileInputStream(file));
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " is starting for file=" + file);
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(),e);
            }
        }
        return conn;
    }

    public void release()
    {
        if (commitOnRelease)
        {
            // Non-transactional operation end : Write to file
            try
            {
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " is committing");
                FileOutputStream os = new FileOutputStream(file);
                ((Workbook)conn).write(new FileOutputStream(file));
                os.close();
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " committed connection");
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(),e);
            }
        }
        super.release();
    }

    public void close()
    {
        if (conn == null)
        {
            return;
        }

        try
        {
            for (int i=0; i<listeners.size(); i++)
            {
                listeners.get(i).managedConnectionPreClose();
            }

            // Commit any remaining changes
            NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " is committing");
            FileOutputStream os = new FileOutputStream(file);
            ((Workbook)conn).write(new FileOutputStream(file));
            os.close();
            NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " committed connection");

            // Close the connection
            file = null;
            conn = null;
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(),e);
        }
        finally
        {
            for (int i=0; i<listeners.size(); i++)
            {
                listeners.get(i).managedConnectionPostClose();
            }
        }

        super.close();
    }

    public XAResource getXAResource()
    {
        return null;
    }
}
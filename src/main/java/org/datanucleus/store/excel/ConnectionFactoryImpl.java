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

import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Implementation of a ConnectionFactory for Excel documents.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    String filename = null;
    boolean ooxml = false;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);
        // "excel:file:{filename}"/"ooxml:file:{filename}"/"xls:file:{filename}"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "' (or alias)");
        }

        int filenameStart = 6;
        if (url.startsWith("excel:"))
        {
            ooxml = false;
        }
        else if (url.startsWith("ooxml:"))
        {
            ooxml = true;
        }
        else if (url.startsWith("xls:"))
        {
            ooxml = false;
            filenameStart = 4;
        }
        else
        {
            throw new NucleusException("invalid URL: "+url);
        }

        // Split the URL into filename
        String str = url.substring(filenameStart); // Omit "excel:"/"ooxml:"/"xls:"
        if (str.indexOf("file:") != 0)
        {
            throw new NucleusException("invalid URL: "+url);
        }

        filename = str.substring("file:".length()); // Omit "file:"
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction
     * associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param options options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        return ooxml ? new OOXMLManagedConnection(filename) : new XLSManagedConnection(filename);
    }
}
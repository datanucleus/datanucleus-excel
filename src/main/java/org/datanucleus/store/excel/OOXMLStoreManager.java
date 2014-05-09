/**********************************************************************
Copyright (c) 2010 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.excel;

import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.util.ClassUtils;

/**
 * StoreManager for data access to OOXML documents.
 * Makes use of Apache POI project.
 */
public class OOXMLStoreManager extends ExcelStoreManager
{
    public OOXMLStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("ooxml", clr, ctx, props);

        // Check if Apache POI OOXML JAR is in CLASSPATH
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.apache.poi.xssf.usermodel.XSSFWorkbook", "poi-ooxml.jar");
    }
}
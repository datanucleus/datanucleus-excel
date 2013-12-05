/**********************************************************************
Copyright (c) 2010 Guido Anzuoni and others. All rights reserved.
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
2011 Andy Jefferson - remove lots of duplication from StoreFieldManager
 ...
***********************************************************************/
package org.datanucleus.store.excel.fieldmanager;

import org.apache.poi.ss.usermodel.Row;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.excel.ExcelUtils;

/**
 * FieldManager to handle the store information for an embedded persistable object into Excel.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    AbstractMemberMetaData embeddedMetaData;

    public StoreEmbeddedFieldManager(ObjectProvider op, Row row, AbstractMemberMetaData mmd, boolean insert)
    {
        super(op, row, insert);
        embeddedMetaData = mmd;
    }

    protected int getColumnIndexForMember(int memberNumber)
    {
        return ExcelUtils.getColumnIndexForFieldOfEmbeddedClass(op.getClassMetaData(),memberNumber, embeddedMetaData);
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
    	EmbeddedMetaData emd = embeddedMetaData.getEmbeddedMetaData();
    	AbstractMemberMetaData []emb_mmd = emd.getMemberMetaData();
        AbstractMemberMetaData mmd = emb_mmd[fieldNumber];
        if (!isStorable(mmd))
        {
            return;
        }

        RelationType relationType = mmd.getRelationType(op.getExecutionContext().getClassLoaderResolver());
        if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
        {
            // Persistable object embedded into this table
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                ObjectProvider embSM = null;
                if (value != null)
                {
                    embSM = ec.findObjectProviderForEmbedded(value, op, mmd);
                }
                else
                {
                    embSM = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                }

                embSM.provideFields(embcmd.getAllMemberPositions(), new StoreEmbeddedFieldManager(embSM, row, mmd, insert));
                return;
            }
        }

        storeObjectFieldInCell(fieldNumber, value, mmd, clr);
    }
}
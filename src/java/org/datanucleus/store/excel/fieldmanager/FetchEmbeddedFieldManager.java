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
2011 Andy Jefferson - remove lots of duplication from FetchFieldManager
 ...
***********************************************************************/
package org.datanucleus.store.excel.fieldmanager;

import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Sheet;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;

/**
 * FieldManager to handle the retrieval of information for an embedded persistable object from a row of Excel.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(ExecutionContext ec, Sheet sheet, int row, AbstractClassMetaData cmd, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, sheet, row, table);
        this.mmds = mmds;
    }

    public FetchEmbeddedFieldManager(ObjectProvider op, Sheet sheet, int row, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(op, sheet, row, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
    	AbstractMemberMetaData[] embMmd = embmd.getMemberMetaData();
        AbstractMemberMetaData mmd = embMmd[fieldNumber];
        RelationType relationType = mmd.getRelationType(clr);

        if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
        {
            // Persistable object embedded into this table
            List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
            embMmds.add(mmd);
            AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            ObjectProvider embOP = ec.newObjectProviderForEmbedded(embCmd, op, fieldNumber);
            FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, sheet, row, embMmds, table);
            embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
            return embOP.getObject();
        }

        return fetchObjectFieldFromCell(fieldNumber, mmd, clr, relationType);
    }
}
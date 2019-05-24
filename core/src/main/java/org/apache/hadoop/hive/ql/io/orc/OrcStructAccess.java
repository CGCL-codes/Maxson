package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zyp
 */
public class OrcStructAccess {

   public static OrcStruct getOrcStruct(int child){
       return new OrcStruct(child);
   }

   public   static  Object getFieldValue(OrcStruct value,int fieldIndex) {
        return value.getFieldValue(fieldIndex);
    }

    public static void setFieldValue(OrcStruct orcStruct,int fieldIndex,Object value){
       orcStruct.setFieldValue(fieldIndex,value);
    }
}

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * create with org.apache.hadoop.hive.ql.io.orc
 * USER: husterfox
 */
public class JsonDeserializer extends OrcSerde {
    private ObjectInspector inspector = null;

    @Override
    public void initialize(Configuration conf, Properties table) {
        // Read the configuration parameters
        String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
        // NOTE: if "columns.types" is missing, all columns will be of String type
        String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        String allColumnName = conf.get("spark.hive.cache.json.col.order");

        // Parse the configuration parameters
        ArrayList<String> columnNames = new ArrayList<String>();
        if (allColumnName != null && allColumnName.length() > 0) {
            columnNames.addAll(Arrays.asList(allColumnName.split(",")));
        } else {
            if (columnNameProperty != null && columnNameProperty.length() > 0) {
                columnNames.addAll(Arrays.asList(columnNameProperty.split(",")));
            }
        }
        if (columnTypeProperty == null) {
            // Default type: all string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnNames.size(); i++) {
                if (i > 0) {
                    sb.append(":");
                }
                sb.append("string");
            }
            columnTypeProperty = sb.toString();
        } else {
            if (allColumnName != null) {
                StringBuilder sb = new StringBuilder();
                String[] names = allColumnName.split(",");
                String[] dataColName = new String[0];
                String[] dataColType = new String[0];
                if(columnNameProperty != null){
                    dataColName = columnNameProperty.split(",");
                }
                dataColType = columnTypeProperty.split(":");
                HashMap<String, String> name2Type = new HashMap<>();
                for (int i = 0; i < dataColName.length; i++) {
                    name2Type.put(dataColName[i], dataColType[i]);
                }
                for (int i = 0; i < names.length; i++) {
                    if (i > 0) {
                        sb.append(":");
                    }
                    sb.append(name2Type.getOrDefault(names[i], "string"));
                }
                columnTypeProperty = sb.toString();
            }
        }

        ArrayList<TypeInfo> fieldTypes =
                TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        StructTypeInfo rootType = new StructTypeInfo();
        rootType.setAllStructFieldNames(columnNames);
        rootType.setAllStructFieldTypeInfos(fieldTypes);
        inspector = OrcStruct.createObjectInspector(rootType);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

}

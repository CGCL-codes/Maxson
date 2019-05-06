//package org.apache.spark.examples;
//
//import com.clearspring.analytics.util.Lists;
//import org.apache.hadoop.hive.ql.exec.Utilities;
//import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
//import org.apache.hadoop.hive.ql.plan.*;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
//
//import java.util.List;
//
//import static javolution.testing.TestContext.assertEquals;
//import static javolution.testing.TestContext.assertNotNull;
//import static javolution.testing.TestContext.fail;
//
///**
// * @author zyp
// */
//public class OrcPushDown {
//
//    @Test
//    public void getRowIDSearchCondition() {
//        setup();
//        ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid", null, false);
//        ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "hi");
//        List<ExprNodeDesc> children = Lists.newArrayList();
//        children.add(column);
//        children.add(constant);
//        ExprNodeDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, new GenericUDFOPEqual(), children);
//        assertNotNull(node);
//        String filterExpr = Utilities.serializeExpression(node);
//        conf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);
//        try {
//            List<IndexSearchCondition> sConditions = handler.getSearchConditions(conf);
//            assertEquals(sConditions.size(), 1);
//        } catch (Exception e) {
//            fail("Error getting search conditions");
//        }
//    }
//}

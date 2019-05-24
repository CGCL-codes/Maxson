/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, MetadataBuilder}
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
  * The Hive table scan operator.  Column and partition pruning are both handled.
  *
  * @param requestedAttributes  Attributes to be fetched from the Hive table.
  * @param relation             The Hive table be scanned.
  * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
  */
private[hive]
case class HiveTableScanExec(
                              requestedAttributes: Seq[Attribute],
                              relation: HiveTableRelation,
                              partitionPruningPred: Seq[Expression])(
                              @transient private val sparkSession: SparkSession)
  extends LeafExecNode with CastSupport {

  require(partitionPruningPred.isEmpty || relation.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")

  override def conf: SQLConf = sparkSession.sessionState.conf

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPred.flatMap(_.references))

  private val originalAttributes = AttributeMap(relation.output.map(a => a -> a))
  private val originalAttributesIncludeJson = mutable.HashMap.empty[ExprId, Attribute]
  relation.output.foreach(a => originalAttributesIncludeJson.update(a.exprId, a))
  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    //requestedAttributes.map(originalAttributes)
    val originalIds = requestedAttributes.filter(!_.name.contains(":")).map(_.exprId)
    val originalCols = relation.output.filter(item => originalIds.contains(item.exprId))
    val jsonCols = requestedAttributes.filter(_.name.contains(":")).map { attr =>
      assert(attr.name.contains(":"))
      val function = attr.metadata.getString("function")
      val rootId = ExprId(attr.metadata.getLong("rootId"))
      val root = originalAttributesIncludeJson(rootId).name
      val field = attr.metadata.getString("field")
      val castTypeSuffix = if (attr.metadata.contains("castType")) {
        s":${attr.metadata.getString("castType")}"
      } else {
        ""
      }
      val name = s"$function:$root:$field$castTypeSuffix"
      val metadata = new MetadataBuilder()
        .withMetadata(attr.metadata)
        .putString("root", root)
        .build()
      val newAttr = attr.withName(name).withMetadata(metadata)
      originalAttributesIncludeJson.update(newAttr.exprId, newAttr)
      newAttr
    }
    println(s"originalTableSchema: ${relation.output}")
    println(s"***originalCols: *****************$originalCols ************************")
    println(s"***JsonCols: *****************$jsonCols ************************")

    originalCols ++ jsonCols
  }

  //json列的名字 s"$function:$root:$field$castTypeSuffix"
  //其余列的名字就是其余列
  private val columsNames = schema.fieldNames
  private val (jsonFiledSchema, tableSchema) = schema.fields.partition(_.name.contains(":"))


  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private lazy val boundPruningPred = partitionPruningPred.reduceLeftOption(And).map { pred =>
    require(
      pred.dataType == BooleanType,
      s"Data type of predicate $pred must be BooleanType rather than ${pred.dataType}.")

    BindReferences.bindReference(pred, relation.partitionCols)
  }

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(relation.tableMeta)
  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  // Create a local copy of hadoopConf,so that scan specific modifications should not impact
  // other queries
  @transient private lazy val hadoopConf = {
    val c = sparkSession.sessionState.newHadoopConf()
    // append columns ids and names before broadcast
    addColumnMetadataToConf(c)
    c
  }

  @transient private lazy val hadoopReader = new HadoopTableReader(
    output,
    relation.partitionCols,
    tableDesc,
    sparkSession,
    hadoopConf)

  private def castFromString(value: String, dataType: DataType) = {
    cast(Literal(value), dataType).eval(null)
  }

  private def addColumnMetadataToConf(hiveConf: Configuration): Unit = {
    //    spark.hive.cache.json.table "table_name"
    //    spark.hive.cache.json.database "database"
    //    spark.hive.cache.json.keys  "key0,key1,key2"
    //    spark.hive.cache.json.cols "col0,col1,col2"
    //    spark.hive.cache.json.col.order "col0,col1,col2,col3,col4,col5"
    if (sparkSession.sparkContext.conf.getBoolean("spark.sql.json.optimize", false)) {
      hiveConf.set("spark.hive.cache.json.database", relation.tableMeta.database)
      hiveConf.set("spark.hive.cache.json.table", relation.tableMeta.identifier.table)
      val jsonKeys = jsonFiledSchema.map(_.metadata.getString("field")).mkString(",")
      hiveConf.set("spark.hive.cache.json.keys", jsonKeys)
      val jsonCols = jsonFiledSchema.map(_.metadata.getString("root")).mkString(",")
      hiveConf.set("spark.hive.cache.json.cols", jsonCols)
      val colOrder = schema.map(item => if (item.name.contains(":")) {
        item.metadata.getString("field")
      } else {
        item.name
      }).mkString(",")
      val duplicate = colOrder.split(",").map((_, 1)).groupBy(_._1).map(elem => (elem._1, elem._2.map(_._2).sum)).filter(_._2 > 1)
      if (duplicate.nonEmpty) {
        throw new QueryExecutionException(s"cols has some duplicate:$duplicate")
      }
      hiveConf.set("spark.hive.cache.json.col.order", colOrder)
      logInfo(s"******database: ${relation.tableMeta.database} table: ${relation.tableMeta.identifier.table}  jsonKeys: $jsonKeys jsonCols: $jsonCols colOrder: $colOrder")
    }
    // Specifies needed column IDs for those non-partitioning columns.
    val columnOrdinals = AttributeMap(relation.dataCols.zipWithIndex)
    //GetJsonObject引用的列不会被列到neededColumnIDs
    val neededColumnIDs = output.flatMap(columnOrdinals.get).map(o => o: Integer)
    //过滤掉output中GetJsonObject列
    HiveShim.appendReadColumns(hiveConf, neededColumnIDs, output.filter(!_.name.contains(":")).map(_.name))

    val deserializer = tableDesc.getDeserializerClass.newInstance
    deserializer.initialize(hiveConf, tableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs.asScala
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, relation.dataCols.map(_.name).mkString(","))
  }

  /**
    * Prunes partitions not involve the query plan.
    *
    * @param partitions All partitions of the relation.
    * @return Partitions that are involved in the query plan.
    */
  private[hive] def prunePartitions(partitions: Seq[HivePartition]) = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = relation.partitionCols.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  // exposed for tests
  @transient lazy val rawPartitions = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPred.size > 0) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        val normalizedFilters = partitionPruningPred.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        sparkSession.sessionState.catalog.listPartitionsByFilter(
          relation.tableMeta.identifier,
          normalizedFilters)
      } else {
        sparkSession.sessionState.catalog.listPartitions(relation.tableMeta.identifier)
      }
    prunedPartitions.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Using dummyCallSite, as getCallSite can turn out to be expensive with
    // with multiple partitions.
    val rdd = if (!relation.isPartitioned) {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        hadoopReader.makeRDDForTable(hiveQlTable)
      }
    } else {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        hadoopReader.makeRDDForPartitionedTable(prunePartitions(rawPartitions))
      }
    }
    val numOutputRows = longMetric("numOutputRows")
    // Avoid to serialize MetastoreRelation because schema is lazy. (see SPARK-15649)
    val outputSchema = schema
    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(outputSchema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def doCanonicalize(): HiveTableScanExec = {
    val input: AttributeSeq = relation.output
    HiveTableScanExec(
      requestedAttributes.map(QueryPlan.normalizeExprId(_, input)),
      relation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(partitionPruningPred, input))(sparkSession)
  }

  override def otherCopyArgs: Seq[AnyRef] = Seq(sparkSession)
}

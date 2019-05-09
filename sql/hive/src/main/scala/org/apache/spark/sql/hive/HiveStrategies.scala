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

package org.apache.spark.sql.hive

import java.io.IOException
import java.util.Locale

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Strategy, _}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoDir, InsertIntoTable, LogicalPlan, ScriptTransformation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{CreateTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Determine the database, serde/format and schema of the Hive serde table, according to the storage
  * properties.
  */
class ResolveHiveSerdeTable(session: SparkSession) extends Rule[LogicalPlan] {
  private def determineHiveSerde(table: CatalogTable): CatalogTable = {
    if (table.storage.serde.nonEmpty) {
      table
    } else {
      if (table.bucketSpec.isDefined) {
        throw new AnalysisException("Creating bucketed Hive serde table is not supported yet.")
      }

      val defaultStorage = HiveSerDe.getDefaultStorage(session.sessionState.conf)
      val options = new HiveOptions(table.storage.properties)

      val fileStorage = if (options.fileFormat.isDefined) {
        HiveSerDe.sourceToSerDe(options.fileFormat.get) match {
          case Some(s) =>
            CatalogStorageFormat.empty.copy(
              inputFormat = s.inputFormat,
              outputFormat = s.outputFormat,
              serde = s.serde)
          case None =>
            throw new IllegalArgumentException(s"invalid fileFormat: '${options.fileFormat.get}'")
        }
      } else if (options.hasInputOutputFormat) {
        CatalogStorageFormat.empty.copy(
          inputFormat = options.inputFormat,
          outputFormat = options.outputFormat)
      } else {
        CatalogStorageFormat.empty
      }

      val rowStorage = if (options.serde.isDefined) {
        CatalogStorageFormat.empty.copy(serde = options.serde)
      } else {
        CatalogStorageFormat.empty
      }

      val storage = table.storage.copy(
        inputFormat = fileStorage.inputFormat.orElse(defaultStorage.inputFormat),
        outputFormat = fileStorage.outputFormat.orElse(defaultStorage.outputFormat),
        serde = rowStorage.serde.orElse(fileStorage.serde).orElse(defaultStorage.serde),
        properties = options.serdeProperties)

      table.copy(storage = storage)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case c@CreateTable(t, _, query) if DDLUtils.isHiveTable(t) =>
      // Finds the database name if the name does not exist.
      val dbName = t.identifier.database.getOrElse(session.catalog.currentDatabase)
      val table = t.copy(identifier = t.identifier.copy(database = Some(dbName)))

      // Determines the serde/format of Hive tables
      val withStorage = determineHiveSerde(table)

      // Infers the schema, if empty, because the schema could be determined by Hive
      // serde.
      val withSchema = if (query.isEmpty) {
        val inferred = HiveUtils.inferSchema(withStorage)
        if (inferred.schema.length <= 0) {
          throw new AnalysisException("Unable to infer the schema. " +
            s"The schema specification is required to create the table ${inferred.identifier}.")
        }
        inferred
      } else {
        withStorage
      }

      c.copy(tableDesc = withSchema)
  }
}

class DetermineTableStats(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case relation: HiveTableRelation
      if DDLUtils.isHiveTable(relation.tableMeta) && relation.tableMeta.stats.isEmpty =>
      val table = relation.tableMeta
      val sizeInBytes = if (session.sessionState.conf.fallBackToHdfsForStatsEnabled) {
        try {
          val hadoopConf = session.sessionState.newHadoopConf()
          val tablePath = new Path(table.location)
          val fs: FileSystem = tablePath.getFileSystem(hadoopConf)
          fs.getContentSummary(tablePath).getLength
        } catch {
          case e: IOException =>
            logWarning("Failed to get table size from hdfs.", e)
            session.sessionState.conf.defaultSizeInBytes
        }
      } else {
        session.sessionState.conf.defaultSizeInBytes
      }

      val withStats = table.copy(stats = Some(CatalogStatistics(sizeInBytes = BigInt(sizeInBytes))))
      relation.copy(tableMeta = withStats)
  }
}

/**
  * Replaces generic operations with specific variants that are designed to work with Hive.
  *
  * Note that, this rule must be run after `PreprocessTableCreation` and
  * `PreprocessTableInsertion`.
  */
object HiveAnalysis extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case InsertIntoTable(r: HiveTableRelation, partSpec, query, overwrite, ifPartitionNotExists)
      if DDLUtils.isHiveTable(r.tableMeta) =>
      InsertIntoHiveTable(r.tableMeta, partSpec, query, overwrite,
        ifPartitionNotExists, query.output)

    case CreateTable(tableDesc, mode, None) if DDLUtils.isHiveTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc)
      CreateTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)

    case CreateTable(tableDesc, mode, Some(query)) if DDLUtils.isHiveTable(tableDesc) =>
      DDLUtils.checkDataColNames(tableDesc)
      CreateHiveTableAsSelectCommand(tableDesc, query, query.output, mode)

    case InsertIntoDir(isLocal, storage, provider, child, overwrite)
      if DDLUtils.isHiveTable(provider) =>
      val outputPath = new Path(storage.locationUri.get)
      if (overwrite) DDLUtils.verifyNotReadPath(child, outputPath)

      InsertIntoHiveDirCommand(isLocal, storage, child, overwrite, child.output)
  }
}

/**
  * Relation conversion from metastore relations to data source relations for better performance
  *
  * - When writing to non-partitioned Hive-serde Parquet/Orc tables
  * - When scanning Hive-serde Parquet/ORC tables
  *
  * This rule must be run before all other DDL post-hoc resolution rules, i.e.
  * `PreprocessTableCreation`, `PreprocessTableInsertion`, `DataSourceAnalysis` and `HiveAnalysis`.
  */
case class RelationConversions(
                                conf: SQLConf,
                                sessionCatalog: HiveSessionCatalog) extends Rule[LogicalPlan] {
  private def isConvertible(relation: HiveTableRelation): Boolean = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    serde.contains("parquet") && conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET) ||
      serde.contains("orc") && conf.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  private def convert(relation: HiveTableRelation): LogicalRelation = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    if (serde.contains("parquet")) {
      val options = relation.tableMeta.storage.properties + (ParquetOptions.MERGE_SCHEMA ->
        conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING).toString)
      sessionCatalog.metastoreCatalog
        .convertToLogicalRelation(relation, options, classOf[ParquetFileFormat], "parquet")
    } else {
      val options = relation.tableMeta.storage.properties
      if (conf.getConf(SQLConf.ORC_IMPLEMENTATION) == "native") {
        sessionCatalog.metastoreCatalog.convertToLogicalRelation(
          relation,
          options,
          classOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat],
          "orc")
      } else {
        sessionCatalog.metastoreCatalog.convertToLogicalRelation(
          relation,
          options,
          classOf[org.apache.spark.sql.hive.orc.OrcFileFormat],
          "orc")
      }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // Write path
      case InsertIntoTable(r: HiveTableRelation, partition, query, overwrite, ifPartitionNotExists)
        // Inserting into partitioned table is not supported in Parquet/Orc data source (yet).
        if query.resolved && DDLUtils.isHiveTable(r.tableMeta) &&
          !r.isPartitioned && isConvertible(r) =>
        InsertIntoTable(convert(r), partition, query, overwrite, ifPartitionNotExists)

      // Read path
      case relation: HiveTableRelation
        if DDLUtils.isHiveTable(relation.tableMeta) && isConvertible(relation) =>
        convert(relation)
    }
  }
}

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformationExec(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  /**
    * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
    * applied.
    */
  object HiveTableScans extends Strategy {
    //_1 relation
    //_2 col name
    //_3 json path
    private type JsonInfo = (HiveTableRelation, AttributeReference, String)
    private val stringConverter = CatalystTypeConverters.createToScalaConverter(StringType)

    private type ReplaceMap = mutable.HashMap[String, AttributeReference]
    private type ReplaceFunc = (Expression, ReplaceMap) => Option[AttributeReference]


    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: HiveTableRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionCols)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
            predicate.references.subsetOf(partitionKeyIds)
        }
        //collect json info
//        val (jsonInfoSetInProject, jsonInfoSetInFilter) = collectJsonInfo(projectList, otherPredicates, relation)
//        println(s"jsonInfoInProject: $jsonInfoSetInProject |||| jsonInfoSetInFilter: $jsonInfoSetInFilter")

        val (scanProjectList, scanFilters) = {
          val conf = sparkSession.sparkContext.conf
          val funcs = ArrayBuffer.empty[ReplaceFunc]
          funcs.append(replaceJson)
          if (funcs.nonEmpty) {
            flattenProject(projectList, predicates, funcs)
          } else {
            (projectList, predicates)
          }
        }


        println(s"scanProjectList: ${scanProjectList}")
        println(s"scanFilters: ${scanFilters}")


        pruneFilterProject(
          scanProjectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, pruningPredicates,projectList.flatMap(_.references))(sparkSession)) :: Nil
      case _ =>
        Nil
    }


    private def mkAttribute(
                             function: String,
                             root: AttributeReference,
                             field: String,
                             attrMap: ReplaceMap): AttributeReference = {
      val attrName = s"$function:${root.name}:$field"
      val key = s"${root.exprId}$field"
      val qualifier = s"${root.qualifier.fold("")(_ + ".")}${root.name}"
      //TODO: add castType
      val metadata = new MetadataBuilder()
        .putString("function", function)
        .putLong("rootId", root.exprId.id)
        .putString("field", field)
        .putString("root",root.name)
        .build()
      attrMap.getOrElseUpdate(key, AttributeReference(
        attrName, StringType, metadata = metadata)(qualifier = Some(qualifier)))
    }
    //把GetJsonObject变成AttributeReference
    private def replaceJson(expr: Expression, attrMap: ReplaceMap): Option[AttributeReference] = {
      expr match {
        case GetJsonObject(r: AttributeReference, Literal(v, StringType)) =>
          var path = stringConverter(v).asInstanceOf[String]
          Some(mkAttribute("get_json_object", r, path, attrMap))
        case _ => None
      }
    }

    private def flattenProject(
                                projectList: Seq[NamedExpression],
                                filterPredicates: Seq[Expression],
                                replaceFuncs: Seq[ReplaceFunc]): (Seq[NamedExpression], Seq[Expression]) = {
      val attrMap = mutable.HashMap.empty[String, AttributeReference]

      def matchExpr(expr: Expression): Option[AttributeReference] = {
        replaceFuncs.foreach { func =>
          func(expr, attrMap) match {
            case Some(attr) => return Some(attr)
            case _ =>
          }
        }
        None
      }

      def replace(expr: Expression): Expression = matchExpr(expr) match {
        case Some(attr) => attr
        case None => expr.withNewChildren(expr.children.map(replace))
      }

      val p = projectList.map(replace(_).asInstanceOf[NamedExpression])
      val f = filterPredicates.map(replace)
      (p, f)
    }


    private def collectJsonInfo(projectList: Seq[NamedExpression],
                                filterPredicates: Seq[Expression],
                                relation: HiveTableRelation): (mutable.Set[JsonInfo], mutable.Set[JsonInfo]) = {

      val jsonInfoSetInProject = mutable.Set[JsonInfo]()
      val jsonInfoSetInFilter = mutable.Set[JsonInfo]()
      projectList.foreach(collect(_, jsonInfoSetInProject, relation))
      filterPredicates.foreach(collect(_, jsonInfoSetInFilter, relation))
      (jsonInfoSetInProject, jsonInfoSetInFilter)
    }

    private def collect(expr: Expression, set: mutable.Set[JsonInfo], relation: HiveTableRelation): Unit = {
      expr.foreach {
        case GetJsonObject(r: AttributeReference, Literal(v, StringType)) => set.add((relation, r, stringConverter(v).asInstanceOf[String]))
        case _ => //do nothing
      }
    }

  }

}

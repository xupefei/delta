/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.{CDC_TYPE_COLUMN_NAME, CDC_TYPE_NOT_CDC, CDC_TYPE_UPDATE_POSTIMAGE, CDC_TYPE_UPDATE_PREIMAGE}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, If, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions.{array, col, explode, input_file_name, lit, struct}
import org.apache.spark.sql.types.LongType

/**
 * Performs an Update using `updateExpression` on the rows that match `condition`
 *
 * Algorithm:
 *   1) Identify the affected files, i.e., the files that may have the rows to be updated.
 *   2) Scan affected files, apply the updates, and generate a new DF with updated rows.
 *   3) Use the Delta protocol to atomically write the new DF as new files and remove
 *      the affected files that are identified in step 1.
 */
case class UpdateCommand(
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
  extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("num_affected_rows", LongType)())
  }

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numAddedBytes" -> createMetric(sc, "number of bytes added"),
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numRemovedBytes" -> createMetric(sc, "number of bytes removed"),
    "numUpdatedRows" -> createMetric(sc, "number of rows updated."),
    "numCopiedRows" -> createMetric(sc, "number of rows copied."),
    "executionTimeMs" ->
      createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
      createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createTimingMetric(sc, "time taken to rewrite the matched files"),
    "numAddedChangeFiles" -> createMetric(sc, "number of change data capture files generated"),
    "changeFileBytes" -> createMetric(sc, "total size of change data capture files generated"),
    "numTouchedRows" -> createMetric(sc, "number of rows touched (copied + updated)")
  )

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.update") {
      val deltaLog = tahoeFileIndex.deltaLog
      deltaLog.withNewTransaction { txn =>
        DeltaLog.assertRemovable(txn.snapshot)
        if (hasBeenExecuted(txn, sparkSession)) {
          sendDriverMetrics(sparkSession, metrics)
          return Seq.empty
        }
        performUpdate(sparkSession, deltaLog, txn)
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }
    Seq(Row(metrics("numUpdatedRows").value))
  }

  private def performUpdate(
      sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction): Unit = {
    import org.apache.spark.sql.delta.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var numAddedBytes: Long = 0
    var numRemovedBytes: Long = 0
    var numAddedChangeFiles: Long = 0
    var changeFileBytes: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0
    var numDeletionVectorsAdded: Option[Long] = None
    var numDeletionVectorsRemoved: Option[Long] = None
    var numDeletionVectorsUpdated: Option[Long] = None

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal.TrueLiteral)
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)

    // Should we write the DVs to represent updated rows?
    val shouldWriteDeletionVectors = shouldWritePersistentDeletionVectors(sparkSession, txn)
    val candidateFiles = txn.filterFiles(
      metadataPredicates ++ dataPredicates,
      keepNumRecords = shouldWriteDeletionVectors)

    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

    val allActions: Seq[FileAction] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty
      numTouchedFiles = candidateFiles.size

      // Rewrite all candidateFiles
      val addAndRemoveActions =
        withStatusCode("DELTA", UpdateCommand.rewritingFilesMsg(candidateFiles.size)) {
          if (candidateFiles.nonEmpty) {
            rewriteFiles(
              sparkSession,
              txn,
              tahoeFileIndex.path,
              candidateFiles,
              nameToAddFile,
              updateCondition,
              generateRemoveFileActions = true,
              copyUnmodifiedRows = true)
          } else {
            Nil
          }
        }

      rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs
      val (addActions, removeActions) = addAndRemoveActions.partition(_.isInstanceOf[AddFile])
      numRewrittenFiles = addActions.size
      numAddedBytes = addActions.map(_.getFileSize).sum
      numRemovedBytes = removeActions.map(_.getFileSize).sum
      addAndRemoveActions
    } else {
      // Case 3: Find all the affected files using the user-specified condition

      val fileIndex = new TahoeBatchFileIndex(
        sparkSession, "update", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)

      if (shouldWriteDeletionVectors) {
        // Case 3.1: Update with persistent deletion vectors
        val targetDf = DMLWithDeletionVectorsHelper.createTargetDfForScanningForMatches(
          sparkSession,
          target,
          fileIndex)

        // Does the target table already has DVs enabled? If so, we need to read the table
        // with deletion vectors
        val mustReadDeletionVectors = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot)

        val touchedFiles = DMLWithDeletionVectorsHelper.findTouchedFiles(
          sparkSession,
          txn,
          mustReadDeletionVectors,
          deltaLog,
          targetDf,
          fileIndex,
          updateCondition,
          opName = "UPDATE")

        if (touchedFiles.nonEmpty) {
          val (dvActions, metricMap) = DMLWithDeletionVectorsHelper.processUnmodifiedData(
            sparkSession,
            touchedFiles,
            txn.snapshot)
          metrics("numUpdatedRows").set(metricMap("numModifiedRows"))
          numTouchedFiles = metricMap("numRemovedFiles")
          val dvRewriteStartMs = System.nanoTime()
          val newFiles = rewriteFiles(
            sparkSession,
            txn,
            tahoeFileIndex.path,
            touchedFiles.map(_.fileLogEntry),
            nameToAddFile,
            updateCondition,
            generateRemoveFileActions = false,
            copyUnmodifiedRows = false)
          rewriteTimeMs = (System.nanoTime() - dvRewriteStartMs) / 1000 / 1000

          dvActions ++ newFiles
        } else {
          Nil // Nothing to update
        }
      } else {
        // Case 3.2: Update without persistent deletion vectors. Existing DVs will be materialized
        // Keep everything from the resolved target except a new TahoeFileIndex
        // that only involves the affected files instead of all files.
        val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
        val data = Dataset.ofRows(sparkSession, newTarget)
        val incrUpdatedCountExpr = IncrementMetric(TrueLiteral, metrics("numUpdatedRows"))
        val pathsToRewrite =
          withStatusCode("DELTA", UpdateCommand.FINDING_TOUCHED_FILES_MSG) {
            data.filter(new Column(updateCondition))
              .select(input_file_name())
              .filter(new Column(incrUpdatedCountExpr))
              .distinct()
              .as[String]
              .collect()
          }

        scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

        val touchedFiles =
          pathsToRewrite.map(getTouchedFile(deltaLog.dataPath, _, nameToAddFile)).toSeq

        // Rewrite all touchedFiles
        val addAndRemoveActions =
          withStatusCode("DELTA", UpdateCommand.rewritingFilesMsg(touchedFiles.size)) {
            if (touchedFiles.nonEmpty) {
              rewriteFiles(
                sparkSession,
                txn,
                tahoeFileIndex.path,
                touchedFiles,
                nameToAddFile,
                updateCondition,
                generateRemoveFileActions = true,
                copyUnmodifiedRows = true)
            } else {
              Nil
            }
          }
        rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs
        numTouchedFiles = touchedFiles.length
        val (addActions, removeActions) = addAndRemoveActions.partition(_.isInstanceOf[AddFile])
        numRewrittenFiles = addActions.size
        numAddedBytes = addActions.map(_.getFileSize).sum
        numRemovedBytes = removeActions.map(_.getFileSize).sum
        addAndRemoveActions
      }
    }

    val (changeActions, _) = allActions.partition(_.isInstanceOf[AddCDCFile])
    numAddedChangeFiles = changeActions.size
    changeFileBytes = changeActions.collect { case f: AddCDCFile => f.size }.sum

    metrics("numAddedFiles").set(numRewrittenFiles)
    metrics("numAddedBytes").set(numAddedBytes)
    metrics("numAddedChangeFiles").set(numAddedChangeFiles)
    metrics("changeFileBytes").set(changeFileBytes)
    metrics("numRemovedFiles").set(numTouchedFiles)
    metrics("numRemovedBytes").set(numRemovedBytes)

    metrics("numDeletionVectorsAdded").set(numDeletionVectorsAdded)
    metrics("numDeletionVectorsRemoved").set(numDeletionVectorsRemoved)
    metrics("numDeletionVectorsUpdated").set(numDeletionVectorsUpdated)
    metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    // In the case where the numUpdatedRows is not captured, we can siphon out the metrics from
    // the BasicWriteStatsTracker. This is for case 2 where the update condition contains only
    // metadata predicates and so the entire partition is re-written.
    val outputRows = txn.getMetric("numOutputRows").map(_.value).getOrElse(-1L)
    if (metrics("numUpdatedRows").value == 0 && outputRows != 0 &&
      metrics("numCopiedRows").value == 0) {
      // We know that numTouchedRows = numCopiedRows + numUpdatedRows.
      // Since an entire partition was re-written, no rows were copied.
      // So numTouchedRows == numUpdateRows
      metrics("numUpdatedRows").set(metrics("numTouchedRows").value)
    } else {
      // This is for case 3 where the update condition contains both metadata and data predicates
      // so relevant files will have some rows updated and some rows copied. We don't need to
      // consider case 1 here, where no files match the update condition, as we know that
      // `totalActions` is empty.
      metrics("numCopiedRows").set(
        metrics("numTouchedRows").value - metrics("numUpdatedRows").value)
    }
    txn.registerSQLMetrics(sparkSession, metrics)

    val finalActions = createSetTransaction(sparkSession, deltaLog).toSeq ++ allActions
    txn.commitIfNeeded(finalActions, DeltaOperations.Update(condition))
    sendDriverMetrics(sparkSession, metrics)

    recordDeltaEvent(
      deltaLog,
      "delta.dml.update.stats",
      data = UpdateMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numTouchedFiles,
        numRewrittenFiles,
        numAddedChangeFiles,
        changeFileBytes,
        scanTimeMs,
        rewriteTimeMs,
        numDeletionVectorsAdded,
        numDeletionVectorsRemoved,
        numDeletionVectorsUpdated)
    )
  }

  /**
   * Scan all the affected files and write out the updated files.
   *
   * When CDF is enabled, includes the generation of CDC preimage and postimage columns for
   * changed rows.
   *
   * @return a list of [[FileAction]]s, consisting of newly-written data and CDC files and old
   *         files that have been removed.
   */
  private def rewriteFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      rootPath: Path,
      inputLeafFiles: Seq[AddFile],
      nameToAddFileMap: Map[String, AddFile],
      condition: Expression,
      generateRemoveFileActions: Boolean,
      copyUnmodifiedRows: Boolean): Seq[FileAction] = {
    // Number of total rows that we have seen, i.e. are either copying or updating (sum of both).
    // This will be used later, along with numUpdatedRows, to determine numCopiedRows.
    val incrTouchedCountExpr = IncrementMetric(TrueLiteral, metrics("numTouchedRows"))

    // Containing the map from the relative file path to AddFile
    val baseRelation = buildBaseRelation(
      spark, txn, "update", rootPath, inputLeafFiles.map(_.path), nameToAddFileMap)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
    val targetDf = Dataset.ofRows(spark, newTarget)
    val targetDfWithEvaluatedCondition = {
      val evalDf = targetDf.withColumn(UpdateCommand.CONDITION_COLUMN_NAME, new Column(condition))
      val copyAndUpdateRowsDf = if (copyUnmodifiedRows) {
        evalDf
      } else {
        evalDf.filter(new Column(UpdateCommand.CONDITION_COLUMN_NAME))
      }
      copyAndUpdateRowsDf.filter(new Column(incrTouchedCountExpr))
    }

    val updatedDataFrame = UpdateCommand.withUpdatedColumns(
      target.output,
      updateExpressions,
      condition,
      targetDfWithEvaluatedCondition,
      UpdateCommand.shouldOutputCdc(txn))

    val addFiles = txn.writeFiles(updatedDataFrame)

    val removeFiles = if (generateRemoveFileActions) {
      val operationTimestamp = System.currentTimeMillis()
      inputLeafFiles.map(_.removeWithTimestamp(operationTimestamp))
    } else {
      Nil
    }

    addFiles ++ removeFiles
  }

  def shouldWritePersistentDeletionVectors(
      spark: SparkSession, txn: OptimisticTransaction): Boolean = {
    spark.conf.get(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS) &&
      DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
  }
}

object UpdateCommand {
  val FILE_NAME_COLUMN = "_input_file_name_"
  val CONDITION_COLUMN_NAME = "__condition__"
  val FINDING_TOUCHED_FILES_MSG: String = "Finding files to rewrite for UPDATE operation"

  def rewritingFilesMsg(numFilesToRewrite: Long): String =
    s"Rewriting $numFilesToRewrite files for UPDATE operation"

  /**
   * Whether or not CDC is enabled on this table and, thus, if we should output CDC data during this
   * UPDATE operation.
   */
  def shouldOutputCdc(txn: OptimisticTransaction): Boolean = {
    DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(txn.metadata)
  }

  /**
   * Build the new columns. If the condition matches, generate the new value using
   * the corresponding UPDATE EXPRESSION; otherwise, keep the original column value.
   *
   * When CDC is enabled, includes the generation of CDC pre-image and post-image columns for
   * changed rows.
   *
   * @param originalExpressions the original column values
   * @param updateExpressions the update transformation to perform on the input DataFrame
   * @param dfWithEvaluatedCondition source DataFrame on which we will apply the update expressions
   *                                 with an additional column CONDITION_COLUMN_NAME which is the
   *                                 true/false value of if the update condition is satisfied
   * @param condition update condition
   * @param shouldOutputCdc if we should output CDC data during this UPDATE operation.
   * @return the updated DataFrame, with extra CDC columns if CDC is enabled
   */
  def withUpdatedColumns(
      originalExpressions: Seq[Attribute],
      updateExpressions: Seq[Expression],
      condition: Expression,
      dfWithEvaluatedCondition: DataFrame,
      shouldOutputCdc: Boolean): DataFrame = {
    val resultDf = if (shouldOutputCdc) {
      val namedUpdateCols = updateExpressions.zip(originalExpressions).map {
        case (expr, targetCol) => new Column(expr).as(targetCol.name, targetCol.metadata)
      }

      // Build an array of output rows to be unpacked later. If the condition is matched, we
      // generate CDC pre and postimages in addition to the final output row; if the condition
      // isn't matched, we just generate a rewritten no-op row without any CDC events.
      val preimageCols = originalExpressions.map(new Column(_)) :+
        lit(CDC_TYPE_UPDATE_PREIMAGE).as(CDC_TYPE_COLUMN_NAME)
      val postimageCols = namedUpdateCols :+
        lit(CDC_TYPE_UPDATE_POSTIMAGE).as(CDC_TYPE_COLUMN_NAME)
      val notCdcCol = new Column(CDC_TYPE_NOT_CDC).as(CDC_TYPE_COLUMN_NAME)
      val updatedDataCols = namedUpdateCols :+ notCdcCol
      val noopRewriteCols = originalExpressions.map(new Column(_)) :+ notCdcCol
      val packedUpdates = array(
        struct(preimageCols: _*),
        struct(postimageCols: _*),
        struct(updatedDataCols: _*)
      ).expr

      val packedData = if (condition == Literal.TrueLiteral) {
        packedUpdates
      } else {
        If(
          UnresolvedAttribute(CONDITION_COLUMN_NAME),
          packedUpdates, // if it should be updated, then use `packagedUpdates`
          array(struct(noopRewriteCols: _*)).expr) // else, this is a noop rewrite
      }

      // Explode the packed array, and project back out the final data columns.
      val finalColumns = (originalExpressions :+ UnresolvedAttribute(CDC_TYPE_COLUMN_NAME)).map {
        a => col(s"packedData.`${a.name}`").as(a.name, a.metadata)
      }
      dfWithEvaluatedCondition
        .select(explode(new Column(packedData)).as("packedData"))
        .select(finalColumns: _*)
    } else {
      val finalCols = updateExpressions.zip(originalExpressions).map { case (update, original) =>
        val updated = if (condition == Literal.TrueLiteral) {
          update
        } else {
          If(UnresolvedAttribute(CONDITION_COLUMN_NAME), update, original)
        }
        new Column(updated).as(original.name, original.metadata)
      }

      dfWithEvaluatedCondition.select(finalCols: _*)
    }

    resultDf.drop(CONDITION_COLUMN_NAME)
  }
}

/**
 * Used to report details about update.
 *
 * @param condition: what was the update condition
 * @param numFilesTotal: how big is the table
 * @param numTouchedFiles: how many files did we touch
 * @param numRewrittenFiles: how many files had to be rewritten
 * @param numAddedChangeFiles: how many change files were generated
 * @param changeFileBytes: total size of change files generated
 * @param scanTimeMs: how long did finding take
 * @param rewriteTimeMs: how long did rewriting take
 * @param numDeletionVectorsAdded: how many deletion vectors were added
 * @param numDeletionVectorsRemoved: how many deletion vectors were removed
 * @param numDeletionVectorsUpdated: how many deletion vectors were updated
 *
 * @note All the time units are milliseconds.
 */
case class UpdateMetric(
    condition: String,
    numFilesTotal: Long,
    numTouchedFiles: Long,
    numRewrittenFiles: Long,
    numAddedChangeFiles: Long,
    changeFileBytes: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numDeletionVectorsAdded: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numDeletionVectorsUpdated: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numDeletionVectorsRemoved: Option[Long] = None
)

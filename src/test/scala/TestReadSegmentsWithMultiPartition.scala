import java.nio.file.Paths
import java.{lang, util}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.scalatest.FunSuite
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index._
import org.apache.lucene.store.{Directory, FSDirectory}

class TestReadSegmentsWithMultiPartition extends FunSuite {

  object FakeMergePolicy extends MergePolicy {
    override def findMerges(mergeTrigger: MergeTrigger, segmentInfos: SegmentInfos, mergeContext: MergePolicy.MergeContext): MergePolicy.MergeSpecification = null

    override def findForcedMerges(segmentInfos: SegmentInfos, maxSegmentCount: Int, segmentsToMerge: util.Map[SegmentCommitInfo, lang.Boolean], mergeContext: MergePolicy.MergeContext): MergePolicy.MergeSpecification = null

    override def findForcedDeletesMerges(segmentInfos: SegmentInfos, mergeContext: MergePolicy.MergeContext): MergePolicy.MergeSpecification = null
  }

  def mockLucene(path: String): Unit = {
    val analyzer = new StandardAnalyzer
    val directory = FSDirectory.open(Paths.get(path))
    // clear test files.
    for (name <- directory.listAll) {
      directory.deleteFile(name)
    }

    val config = new IndexWriterConfig(analyzer).setMergePolicy(FakeMergePolicy)
    val indexWriter = new IndexWriter(directory, config)

    for (i <- 1 to 100) {
      val doc = new Document
      doc.add(new TextField("title", String.valueOf(i), Field.Store.YES))
      doc.add(new TextField("content", String.valueOf(i), Field.Store.YES))

      indexWriter.addDocument(doc)
      if (i % 10 == 0) {
        indexWriter.commit()
      }
    }
    indexWriter.close()
  }

  test("test read lucene segments with muti-partitions") {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("read lucene segments")
    val sparkContext = new SparkContext(sparkConf)
    val testResourcePaths = getClass.getResource("segments").getPath

    // gen lucene segments.
    mockLucene(testResourcePaths)

    // TODO : default could not read file start with _ ?, let me check it later...
    val dirs = FSDirectory.open(Paths.get(testResourcePaths))
    for (file <- dirs.listAll()) {
      dirs.rename(file, "s" + file)
    }


    val result = sparkContext.wholeTextFiles(testResourcePaths + "/*.si", 10)
      .mapPartitions({ iter =>
        println("partition id : " + TaskContext.getPartitionId())
        println("thread id : " + Thread.currentThread().getId)
        iter
      })
      .collect()

    assert(result.length == 10)
  }
}

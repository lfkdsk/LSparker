import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.scalatest.FunSuite


class TestReadFilesWithMultiPartition extends FunSuite {

  test("test read files with multi-partitions") {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("read files")
    val sparkContext = new SparkContext(sparkConf)
    val testResourcePaths = getClass.getResource("testfiles").getPath

    val result = sparkContext.wholeTextFiles(testResourcePaths, 4)
      .mapPartitions((iter: Iterator[(String, String)]) => {
        println("partition id : " + TaskContext.getPartitionId())
        println("thread id : " + Thread.currentThread().getId)
        iter
      })
      .reduceByKey({
        case (_, content) =>
          content
      })
      .collect()

    for (elem <- result) {
      println(elem)
    }

    assert(result.length == 9)
  }
}

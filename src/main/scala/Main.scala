import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object Main {

  def readFilesWithMultiPartition(sc: SparkContext,
                                  path: String,
                                  minPartitions: Int): Unit = {
    sc.wholeTextFiles(path, minPartitions)
      .foreachPartition((iter: Iterator[(String, String)]) => {
        println("partition id : " + TaskContext.getPartitionId())
        println("thread id : " + Thread.currentThread().getId)
        iter.foreach(println)
      })
  }



  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("read files")
    val sparkContext = new SparkContext(sparkConf)
    readFilesWithMultiPartition(sparkContext, "/Users/liufengkai/Documents/Code/personal-projects/LSparker/src/test/resources/testfiles", 2)
  }
}
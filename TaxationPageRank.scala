import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._

object TaxationPageRank {

  def main(args: Array[String]): Unit = {

    //To run in the cluster
    val sc = SparkSession.builder().master(master = "spark://saint-paul:30280").getOrCreate().sparkContext

    //Run in IDE
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    val lines = sc.textFile("hdfs://saint-paul:30261/input/links-simple-sorted.txt")
    val titles = sc.textFile("hdfs://saint-paul:30261/input/titles-sorted.txt").zipWithIndex().mapValues(x => x + 1).map(_.swap)

    val links = lines.map(s => (s.split(": ")(0), s.split(": ")(1)))
  //Need to calculate amount of lines
    val totalLines = titles.count()
    var ranks = links.mapValues(v => 1.0 / totalLines)

  }

}

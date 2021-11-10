
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._

object IdealPageRank {

  def main(args: Array[String]): Unit = {

    //To run in the cluster
    val sc = SparkSession.builder().master(master="spark://saint-paul:30280").getOrCreate().sparkContext

    //Run in IDE
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext


    val lines = sc.textFile("hdfs://saint-paul:30261/input/links-simple-sorted.txt")
    val titles = sc.textFile("hdfs://saint-paul:30261/input/titles-sorted.txt").zipWithIndex().mapValues(x=>x+1).map(_.swap)

    val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
    //Need to calculate amount of lines
    val totalLines = titles.count()
    var ranks = links.mapValues(v => 1.0 / totalLines)

    for(i <- 1 to 25){
      val tempRank = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val outgoingLinks = urls.split(" ")
          outgoingLinks.map(url => (url, rank / outgoingLinks.length))
      }
      ranks = tempRank.reduceByKey(_+_)
    }

    val sortedRanks = ranks.sortBy(_._2, false)
    val topTen = sortedRanks.take(10)
    val topTenRDD = sc.parallelize(topTen)
    val topTenRDDLong = topTenRDD.map{
      case(x,y) => (x.toLong, y)
    }
    val sortedTen = topTenRDDLong.sortBy(_._2, false)
    //topTenRDD.coalesce(1).saveAsTextFile("hdfs://saint-paul:30261/output/")
    val joined = sortedTen.join(titles)
    joined.saveAsTextFile("hdfs://saint-paul:30261/output/")

  }
}

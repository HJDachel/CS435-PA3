
import org.apache.spark.sql.SparkSession

object WikiBomb {

  def main(args: Array[String]): Unit = {
    //To run in the cluster
    val sc = SparkSession.builder().master(master="spark://saint-paul:30280").getOrCreate().sparkContext

    //Run in IDE
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    //Create the links
    val lines = sc.textFile("hdfs://saint-paul:30261/input/links-simple-sorted.txt")
    val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
    //Create the titles
    val titles = sc.textFile("hdfs://saint-paul:30261/input/titles-sorted.txt").zipWithIndex().mapValues(x=>x+1).map(_.swap)

    val surfTitles = titles.filter(x => x._2.contains("surfing"))
    val surfIndex = surfTitles.map(x => x._1).collect()
    val surfLinks = links.filter{case(k,v) => (surfIndex.contains(k.toLong))}.cache


    val rockyRDD = sc.parallelize(List(("4290745", "4290745")))
    val rockyRDDLong = sc.parallelize(List((4290745l, "Rocky_Mountain_National_Park")))
    val allTitles = surfTitles.union(rockyRDDLong)
    val a = surfLinks.map(x => (x._1, x._2 + " 4290745"))
    val linksLong = a.map{
      case(x,y) => (x.toLong, y)
    }
    val finalLinks = a.union(rockyRDD)
    val totalLines = allTitles.count()
    var ranks = finalLinks.mapValues(v => 1.0 / totalLines)
/*
    val linksLong = links.map{
      case(x,y) => (x.toLong, y)
    }
    val rockyRDD = sc.parallelize(List((4290725l, " ")))
    val surfLink = surfTitles.join(linksLong)
    val surfLinksandRocky = surfLink.union(rockyRDD)
    val totalLines = surfLinksandRocky.count()
    var ranks = surfLinksandRocky.mapValues(v => 1.0 / totalLines)
*/
    for(i <- 1 to 25){
      val tempRank = finalLinks.join(ranks).values.flatMap {
        case (urls, rank) =>
          var outgoingLinks = urls.split(" ")
          outgoingLinks.map(url => (url, rank / outgoingLinks.length))
      }

      ranks = tempRank.reduceByKey(_+_)
    }

    val sortedRanks = ranks.sortBy(_._2, false)
    val topTenRDDLong = sortedRanks.map{
      case(x,y) => (x.toLong, y)
    }
    /*
    val topTen = sortedRanks.take(10)
    val topTenRDD = sc.parallelize(topTen)
    val topTenRDDLong = topTenRDD.map{
      case(x,y) => (x.toLong, y)
    }

    val sortedTen = topTenRDDLong.sortBy(_._2, false)
    //topTenRDD.coalesce(1).saveAsTextFile("hdfs://saint-paul:30261/output/")
    */
    val joined = topTenRDDLong.join(allTitles)
    var sortedJoin = joined.sortBy(_._2._1, false).take(10)
    val sortedJoin2 = sc.parallelize(sortedJoin)
    sortedJoin2.coalesce(1).saveAsTextFile("hdfs://saint-paul:30261/outputWiki/")

  }


}

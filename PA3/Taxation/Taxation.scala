import org.apache.spark.sql.SparkSession


object Taxation {
  def main(args: Array[String]): Unit = {

    val PATH_LINK = args(0)
    val PATH_TITLES = args(1)
    val PATH_OUT = args(2)
    val sc = SparkSession.builder().master("").getOrCreate().sparkContext
    //loads in inputfile
    val lines = sc.textFile(PATH_LINK)
    val links = lines.map(s => (s.split(": ")(0), s.split(":")(1)))
    val NUM_OF_TOTAL_PAGES = links.count
    var ranks = links.mapValues(v => 1.0 / NUM_OF_TOTAL_PAGES)

    for (i <- 1 to 25) {
      val tempRank = links.join(ranks).values.flatMap { case (urls, rank) =>
        val outgoingLinks = urls.split("")
        outgoingLinks.map(url => (url, rank / outgoingLinks.length))
      }
      ranks = tempRank.reduceByKey(_ + _).mapValues(v=>( 0.15/NUM_OF_TOTAL_PAGES)+ 0.85 * v)
    }

    val top10_ranks = ranks.sortBy(_._2, false, 1).zipWithIndex.filter { case (_, indx) => (indx < 10) }.keys

    val titles = sc.textFile(PATH_TITLES).zipWithIndex().mapValues(x => x + 1).map(_.swap);
    val Titles = titles.map(x => (x._1.toString, x._2))

    Titles.join(top10_ranks).map { case (k, (ls, rs)) => (k, ls, rs) }.sortBy(_._3, false).coalesce(1).saveAsTextFile(PATH_OUT)

    sc.stop()

  }
}

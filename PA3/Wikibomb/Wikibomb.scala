import org.apache.spark.sql.SparkSession

object WikiBomb {
  def main(args: Array[String]): Unit ={
    val PATH_LINK = args(0)
    val PATH_TITLES = args(1)
    val PATH_OUT = args(2)
    val sc = SparkSession.builder().master("node:port").getOrCreate().sparkContext

    val titles = sc.textFile(PATH_TITLES).zipWithIndex().mapValues(x=>x+1).map(_.swap);
    val rocky = titles.filter(x => x._2 == "Rocky_Mountain_National_Park").collect()(0)
    val regex = ".*[Ss][Uu][Rr][Ff][Ii][Nn][Gg].*".r
    val titlesWSurf = titles.filter(x => regex.pattern.matcher(x._2).matches)


    val surfIndex = titlesWSurf.map(x => x._1).collect()
    val lines = sc.textFile(PATH_LINK)
    val links = lines.map(s => (s.split(": ")(0), s.split(": ")(1).split(" +")))
    val surfLinks = links.filter { case (k, v) => (surfIndex.contains(k.toInt)) }.cache()
    val newTitles = titlesWSurf.union(sc.parallelize(Seq((rocky._1, rocky._2))))
    val newID = newTitles.reduce((x, y) => if (x._1 > y._1) x else y)._1.toInt + 1


    import Array._
    val newPageIndex = range(newID + 1, newID + 1 + surfIndex.size * 2)
    val webTitles = newTitles.union(sc.parallelize(newPageIndex.map(x => (x.toLong, "fake " + x.toString)).toSeq))

    val webLinks = surfLinks.union(sc.parallelize(newPageIndex.map(x => (x.toString, Array[String]())).toSeq))

    val webLinksWithTarget = webLinks.map(x => (x._1, x._2 :+ rocky._1))

    val webLinksString = webLinksWithTarget.map(x => (x._1, x._2.map(x => x.toString)))
    val finalLinks = webLinksString.union(sc.parallelize(Seq((rocky._1.toString, newPageIndex.map(x => x.toString)))))

    val NUM_OF_TOTAL_PAGES = finalLinks.count

    var ranks = finalLinks.mapValues(v => 1.0 / NUM_OF_TOTAL_PAGES)

    for (i <- 1 to 25) {
      val tempRank = finalLinks.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      ranks = tempRank.reduceByKey(_ + _)
    }

    val bestRanks = ranks.coalesce(1).sortBy(_._2, false).zipWithIndex.filter { case (_, indx) => (indx < 10) }.keys

    val finalTitles = webTitles.map(x => (x._1.toString, x._2))
    val spam = finalTitles.join(bestRanks).map { case (k, (ls, rs)) => (k, ls, rs) }.sortBy(_._3, false).coalesce(1)
    spam.saveAsTextFile(PATH_OUT)

    sc.stop()

  }
}

package ss15Impro3aaaa

import java.util

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object FinalProcessing {

  var inputPath1 = "" //
  var inputPath2 = "" //
  var outputPath = "" //

  def main(args: Array[String]) {

    if (args.length < 2) {
      sys.error("inputArmDataPath and inputUserDataPath and outputRecommendationPath console parameters are missing")
      sys.exit(1)
    }

    outputPath = args(1)
    inputPath1 = args(0)
    //inputPath2 = args(2)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val recomendationsPerUser: DataSet[String] = env.readTextFile(inputPath1)

    val data = recomendationsPerUser

      .map( new MapFunction[String, (String, String)]() {

      def map(in: String): (String, String) = {

        val data = in.replace("(","").replace(")","").split(",")

        (data(0), data(2))
      }
    })


    val allRecomendationsPerUser = data
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + " " + t2._2))
      .map(t => (t._1, t._2.split(" ").distinct))
    .map(t => (t._1, t._2.mkString(" ")))

    allRecomendationsPerUser.writeAsText(outputPath, WriteMode.OVERWRITE)


    /*
    val userPurchaseHistoryData: DataSet[(String, String)] = env.readCsvFile(inputPath2)

    val res = allRecomendationsPerUser
      .join(userPurchaseHistoryData)
      .where(0)
      .equalTo(0)
      .map(t => (t._1._1, t._1._2, t._2._2))
      .map(new MapFunction[(String, String, String), (String, String)] {

        override def map(t: (String, String, String)): (String, String) = {
          var recomendations = t._2.split(" ")
          var purchHistory = t._3.split(" ")

          var result = ""
          val itRec = recomendations.iterator
          val itPurch = recomendations.iterator

          while (itRec.hasNext){

            val nextRecItem = itRec.next()
            var isAlreadyBought = false

            while(itPurch.hasNext){

              val nextPurchItem = itPurch.next()

              if(nextRecItem.equals(nextPurchItem)){
                isAlreadyBought = true
              }
            }

            if (!isAlreadyBought){
              result += nextRecItem
              result += " "
            }


          }

          if (result.length != 0){
            result = result.dropRight(1)
          }

          (t._1, result)
        }

    })

    res.writeAsText("/home/vassil/test/pisnami", WriteMode.OVERWRITE)
    */

    env.execute("Scala AssociationRule Example")

  }
}

class FinalProcessing {

}

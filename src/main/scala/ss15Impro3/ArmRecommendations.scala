package ss15Impro3

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.MapFunction



object ArmRecommendations {

  var inputArmDataPath = "" //
  var inputUserDataPath = "" //
  var outputRecommendationPath = ""

  def main(args: Array[String]) {


    if (args.length < 3) {
      sys.error("inputArmDataPath and inputUserDataPath and outputRecommendationPath console parameters are missing")
      sys.exit(1)
    }

    inputArmDataPath = args(0)
    inputUserDataPath = args(1)
    outputRecommendationPath = args(2)
    println("inputArmDataPath: " + inputArmDataPath)
    println("inputUserDataPath: " + inputUserDataPath)
    println("outputRecommendationPath: " + outputRecommendationPath)

    val env = ExecutionEnvironment.getExecutionEnvironment


    // Read data
    val armData = env.readTextFile(inputArmDataPath + "armData.csv")
    val userPurchaseHistoryData : DataSet[(String, String)] = env.readCsvFile(inputUserDataPath + "userPurchaseHistory.csv")

   val transformedUserPurchaseHistoryData = userPurchaseHistoryData.flatMap(new FlatMapFunction[(String, String), (String, String) ] {
     override def flatMap(t: (String, String), collector: Collector[(String, String)]): Unit = {
       val user = t._1
       val items = t._2

       for (item <- items.split(" ")){
         collector.collect(user, item)
       }
     }
   })

    val armDataChanged = armData.map(new MapFunction[String, (String, String)] {
      override def map(t: String): (String, String) = {

        val infos = t.split("\\] => \\[")
        val left = infos(0).substring(2)

        val index = infos(1).indexOf("]")
        val right = infos(1).dropRight(index).replace(", ", "")
        (left, right)
      }
    })

    val result = transformedUserPurchaseHistoryData.
      joinWithHuge(armDataChanged)
      .where(1)
      .equalTo(0)
      .map(t => (t._1, t._2._2))


    //productData.writeAsCsv(outputRecommendationPath + "/recommendationsPerUser" , "\n", ",", WriteMode.OVERWRITE)
    result.writeAsText(outputRecommendationPath + "/recommendationsPerUser", WriteMode.OVERWRITE)

    env.execute("Execute Arm Recommendations")
  }


}

class ArmRecommendations {

}

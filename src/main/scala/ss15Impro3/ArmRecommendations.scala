package ss15Impro3

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

import scala.util.Sorting

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
    val armData: DataSet[(String, String, String)] = env.readCsvFile(inputArmDataPath, "\n", ";")

    //armData.writeAsText("/home/vassil/test/NEW/NEW")
    // ([1487906, 1499395],[1499395, 1487906, 2456222],80.0)


    val userPurchaseHistoryData: DataSet[(String, String)] = env.readCsvFile(inputUserDataPath)

    //userPurchaseHistoryData.writeAsText("/home/vassil/test/NEW/NEW2")
    //(u-1000052,f-244923 f-1487906 f-1499395 f-2000570 f-2929304)


    val armDataChanged = armData.map(new MapFunction[(String, String, String), (String, String)] {
      override def map(t: (String, String, String)): (String, String) = {

        val itemsLeft= t._1.replace("[","").replace("]","").split(", ")
        var leftItemsWithPrefix = ""
        for (item <- itemsLeft) {
          leftItemsWithPrefix += "f-" + item + " "
        }

        val itemsRight = t._2.replace("[","").replace("]","").split(", ")
        var rightItemsWithPrefix = ""
        for (item <- itemsRight) {
          rightItemsWithPrefix += "f-" + item + " "
        }

        (leftItemsWithPrefix.dropRight(1), rightItemsWithPrefix.dropRight(1))
      }
    })

    //armDataChanged.writeAsText("/home/vassil/test/NEW/NEW", WriteMode.OVERWRITE)
    //(f-1487906, 1499395, f-1499395 f-1487906 f-2456222)


    val recomendationsPerUser = userPurchaseHistoryData.flatMap(

      new RichFlatMapFunction[(String, String), (String, String, String)]() {

        var broadcastedRules: util.List[(String, String)] = null

        override def open(config: Configuration): Unit = {
          // 3. Access the broadcasted DataSet as a Collection
          broadcastedRules = getRuntimeContext().getBroadcastVariable[(String, String)]("armData")
        }

        def flatMap(in: (String, String), out: Collector[(String, String, String)]) = {

          val it = broadcastedRules.iterator()
          while (it.hasNext) {

            val rule = it.next()

            val itemsInRule = rule._1.split(" ")

            val itemsForUser = in._2.split(" ")

            var containsAll = true
            for (itemForRule <- itemsInRule) {

              if (!itemsForUser.contains(itemForRule)){
                containsAll = false
              }
            }

            if (containsAll){
              out.collect(in._1, rule._1, rule._2)
            }

          }

        }

      }).withBroadcastSet(armDataChanged, "armData")

    recomendationsPerUser.writeAsText(outputRecommendationPath + "/recommendationsPerUser", WriteMode.OVERWRITE)
    env.execute("Execute Arm Recommendations")



  }
}

class ArmRecommendations {

}

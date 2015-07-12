package ss15Impro3

import org.apache.flink.api.scala.ExecutionEnvironment

object ArmRecommendations {

  def main(args: Array[String]) {

    var inputArmDataPath = "" //
    var inputUserDataPath = "" //
    var outputRecommendationPath = ""

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




  }


}

class ArmRecommendations {}

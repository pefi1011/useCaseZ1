package ss15Impro3aaaa

import java.util

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object Validation {

  var inputArmDataPath = "" //
  var inputUserDataPath = "" //
  var outputRecommendationPath = ""

  def main(args: Array[String]) {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val armData80: DataSet[(String, String, String)] = env.readCsvFile("/home/vassil/workspace/useCaseZ1/output/CopyOfOldOutputs/RESULTS/AR/Support_7/2/together", "\n", ";")


    val armData20: DataSet[(String, String, String)] = env.readCsvFile("/home/vassil/workspace/useCaseZ1/output/armData/2/together", "\n", ";")


   val res =  armData20.
      joinWithHuge(armData80)
      .where(0,1)
      .equalTo(0,1)
      .map(t => (1 , Math.abs(t._1._3.toDouble - t._2._3.toDouble)))
      .groupBy(0)


     val res1 = res
      .sum(1)
      .writeAsText("/home/vassil/workspace/useCaseZ1/output/validation/sum1/", WriteMode.OVERWRITE)





    env.execute("Execute Arm Recommendations")



  }
}

class Validation {

}

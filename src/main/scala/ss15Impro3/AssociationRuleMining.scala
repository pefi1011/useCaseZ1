package ss15Impro3

import java.util

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object AssociationRuleMining {

  private var inputFilePath: String = ""
  private var outputFilePath: String = ""
  private var maxIterations: String = ""
  private var minSupport: String = ""
  private var isFamilyID: String = ""

  def getDataWithProductId(env: ExecutionEnvironment): DataSet[String] = {

    val salesData: DataSet[String] = env.readTextFile(inputFilePath)
    //val salesFilterData = salesData.filter(_.contains("SALE"))

    //val salesOnly = salesFilterData
    val salesOnly = salesData
      .map( new MapFunction[String, (String, String)]() {

      def map(in: String): (String, String) = {

        val infos = in.split("\\s+")

        (infos(1), infos(3).replace(",", " ").replace("p-", ""))
      }
    })
      // Group by user session
      .distinct
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + " " + t2._2))
      .map(t => t._2)

    salesOnly
  }

  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    if (isFamilyID.equals("1")){
      // Use that if you start algorithm with preprocessed data (with family ids)
      val salesOnlyFamilyId: DataSet[String] = getInputDataPreparedForARM(env, inputFilePath)
      runArm(salesOnlyFamilyId, outputFilePath, maxIterations.toInt, minSupport.toInt)

    } else {
      // Use this data if you start algorithm with the raw data (with product ids)
      val salesOnlyProductId: DataSet[String] = getDataWithProductId(env)
      runArm(salesOnlyProductId, outputFilePath, maxIterations.toInt, minSupport.toInt)

    }

    env.execute("Scala AssociationRule Example")
  }

  // Depending on what type you want to  filter; SALE or VIEW
  def getInputDataPreparedForARM(env: ExecutionEnvironment, input: String): DataSet[String] = {

    val data: DataSet[(String, String, String, String, String)] = env.readCsvFile(input)
    val onlySales = data
      //.filter(_._5.equals("SALE"))
      .map(_._4.replace("f-", "")
      .replace(";", " "))

    return onlySales
  }

  private def runArm(parsedInput: DataSet[String], output: String, maxIterations: Int, minSup: Int): Unit = {
    var kTemp = 1
    var hasConverged = false
    val emptyArray: Array[(String, Int)] = new Array[(String, Int)](0)
    val emptyDS = ExecutionEnvironment.getExecutionEnvironment.fromCollection(emptyArray)
    var preRules: DataSet[(String, Int)] = emptyDS

    // According to how much K steps are, Making Pruned Candidate Set
    while (kTemp < maxIterations && !hasConverged) {
      println()
      printf("Starting K-Path %s\n", kTemp)
      println()

      val candidateRules: DataSet[(String, Int)] = findCandidates(parsedInput, preRules, kTemp, minSup)

      val tempRulesNew = candidateRules

      /* TODO Check if it will work wit the collect
      // TODO Is it ok to collect here?
      val cntRules = candidateRules.collect.length
      */

      if (kTemp >= 2) {

        // TODO Change it with some kind of join with special function
        val confidences: DataSet[(String, String, Double)] = preRules

          //.join(tempRulesNew)

          .crossWithHuge(tempRulesNew)
          .filter { item => containsAllFromPreRule(item._2._1, item._1._1) }

          .map(
            input =>
              (input._1._1, input._2._1, 100 * (input._2._2 / input._1._2.toDouble))
            //RULE: [2, 6] => [2, 4, 6] CONF RATE: 4/6=66.66
          )
        // TODO Should this be here ot in the main function?
        confidences.writeAsCsv(outputFilePath + "/armData/" + kTemp, "\n", ",", WriteMode.OVERWRITE)
      }

      /* TODO comment back only if using wich the collect earlier
      if (0 == cntRules) {
       // hasConverged = true
        kTemp+= 1
      } else {
      */
        preRules = candidateRules

        kTemp += 1
      /*
      }
      */
    }

    printf("Converged K-Path %s\n", kTemp)
  }

  def findCandidates(candidateInput: DataSet[String], prevRulesNew: DataSet[(String, Int)], k: Int, minSup: Int): DataSet[(String, Int)] = {

    // 1) Generating Candidate Set Depending on K Path
    candidateInput.flatMap(

      new RichFlatMapFunction[String, (String, Int)]() {

        var broadcastedPreRules: util.List[(String, Int)] = null

        override def open(config: Configuration): Unit = {
          // 3. Access the broadcasted DataSet as a Collection
          broadcastedPreRules = getRuntimeContext().getBroadcastVariable[(String, Int)]("prevRules")
        }

        def flatMap(in: String, out: Collector[(String, Int)]) = {

          val cItem1: Array[Int] = in.split(" ").map(_.toInt).sorted

          val combGen1 = new CombinationGenerator()
          val combGen2 = new CombinationGenerator()

          combGen1.reset(k, cItem1)

          while (combGen1.hasMoreCombinations) {
            val cItem2 = combGen1.next

            // We assure that the elements will be added in the first iteration. (There are no preRules to compare)
            var valid = true
            if (k > 1) {
              combGen2.reset(k - 1, cItem2)

              // Check if the preRules contain all items of the combGenerator
              while (combGen2.hasMoreCombinations && valid) {
                val nextComb = java.util.Arrays.toString(combGen2.next)

                // TODO If broadcast variable is bad solution then try this -> (BUT) Not serializable exception (These should be the dataset solution)
                // Distributed way for the bottom "for"
                /*
                var containsItemNew : Boolean = prevRulesNew.map{ item =>
                  item._1.equals(nextComb)
                }.reduce(_ || _).collect(0)
                */

                var containsItem = false
                for (i <- 0 to (broadcastedPreRules.size() - 1)) {
                  if (broadcastedPreRules.get(i)._1.equals(nextComb)) {
                    containsItem = true
                  }
                }

                valid = containsItem
              }
            }
            if (valid) {
              out.collect((java.util.Arrays.toString(cItem2), 1))
            }
          }
        }
      })

      .withBroadcastSet(prevRulesNew, "prevRules")
      // 2) Merge Candidate Set on Each Same Word
      .groupBy(0).reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      // 3) Pruning Step
      .filter(_._2 >= minSup)
  }

  private def containsAllFromPreRule(newRule: String, preRule: String): Boolean = {

    // TODO do this some other way
    val newRuleCleaned = newRule.replaceAll("\\s+", "").replaceAll("[\\[\\](){}]", "")
    val preRuleCleaned = preRule.replaceAll("\\s+", "").replaceAll("[\\[\\](){}]", "")
    println(newRule + " " + preRule)

    val newRuleArray = newRuleCleaned.split(",")
    val preRuleArray = preRuleCleaned.split(",")

    var containsAllItems = true

    // Implement that in the filter function
    for (itemOfRule <- preRuleArray) {
      if (!newRuleArray.contains(itemOfRule)) {
        containsAllItems = false
      }
    }
    containsAllItems
  }


  private def parseParameters(args: Array[String]): Boolean = {

    // input, output maxIterations, preOutputPath, kPath, minSupport
    if (args.length == 5) {
      inputFilePath = args(0)
      outputFilePath = args(1)
      maxIterations = args(2)
      minSupport = args(3)
      isFamilyID = args(4)
      true
    } else {
      System.err.println("Usage: AssociationRule <input path> <result path> <iterationCount> <minSupport>")
      false
    }
  }
}
class AssociationRuleMining {

}

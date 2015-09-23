package ss15Impro3

import java.util

import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.api.common.functions.{RichMapFunction, RichGroupReduceFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object AssociationRuleMining {

  @Combinable
  class MyRichGroupReducer extends RichGroupReduceFunction[(String, Int), (String, Int)] {
    def reduce(
                in: java.lang.Iterable[(String, Int)],
                out: Collector[(String, Int)]) : Unit = {

      var key : String = null
      var intSum = 0

      val it = in.iterator()
      while (it.hasNext) {

        val item = it.next()

        key = item._1
        intSum += item._2
      }

      out.collect(key, intSum)
    }

  }

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

  // Depending on what type you want to  filter; SALE or VIEW
  def getInputDataPreparedForARM(env: ExecutionEnvironment, input: String): DataSet[String] = {

    val data: DataSet[(String, String, String, String, String)] = env.readCsvFile(input)
    val onlySales = data

      // We filter SALE during pre-processing
      //.filter(_._5.equals("SALE"))
      .map(_._4.replace("f-", "")
      .replace(";", " "))

    return onlySales
  }


  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }


    val env = ExecutionEnvironment.getExecutionEnvironment

    /*
    val test: DataSet[String] = env.readTextFile("/home/vassil/workspace/useCaseZ1/input/TEST")
    runArm(test, outputFilePath + "/test", maxIterations.toInt, minSupport.toInt)
    */


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

  def checkConf(candidateRules: DataSet[(String, Int)], preRules: DataSet[(String, Int)], d: Double): DataSet[(String, Int)] = {
    candidateRules.flatMap( new RichFlatMapFunction [(String, Int), (String, Int)]() {

      var broadcastedPreRules: util.List[(String, Int)] = null
      override def open(config: Configuration): Unit = {
        // 3. Access the broadcasted DataSet as a Collection
        broadcastedPreRules = getRuntimeContext().getBroadcastVariable[(String, Int)]("prevRules")
      }

      override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {

      }
    })
  }

  private def runArm(parsedInput: DataSet[String], output: String, maxIterations: Int, minSup: Int): Unit = {
    var kTemp = 1
    var support = minSup
    var hasConverged = false
    val emptyArray: Array[(String, Int)] = new Array[(String, Int)](0)
    val emptyDS = ExecutionEnvironment.getExecutionEnvironment.fromCollection(emptyArray)
    var preRules: DataSet[(String, Int)] = emptyDS

    // According to how much K steps are, Making Pruned Candidate Set
    while (kTemp < maxIterations && !hasConverged) {
      println()
      printf("Starting K-Path %s\n", kTemp)
      println()

      val candidateRules: DataSet[(String, Int)] = findCandidates(parsedInput, preRules, kTemp, support)

      //TODO OLD CODE
      val tempRulesNew = candidateRules

      if (kTemp >= 2) {


        //TODO OLD CODE

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
        confidences.writeAsCsv(outputFilePath + "/armData/" + kTemp, "\n", ";", WriteMode.OVERWRITE)



        //preRules.writeAsCsv(outputFilePath + "/armData/" + kTemp, "\n", ";", WriteMode.OVERWRITE)
      }

      preRules = candidateRules

      kTemp += 1

      // TODO Reduce support each three iterations
      if (kTemp % 2 == 0) {
        support -= 1
      }
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

          val cItem1: Array[Int] = in
            .split(" ")
            .map(_.toInt)
            // We sort the ids on the row so they look always the same. The sequence is not important for AR
            .sorted

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

                var containsItem = false

                var i = 0
                while ( i < broadcastedPreRules.size() && !containsItem) {
                  if (broadcastedPreRules.get(i)._1.equals(nextComb)) {
                    containsItem = true
                  }
                  i += 1
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
      // Group reduce

      .groupBy(0)

      .reduceGroup(new MyRichGroupReducer)
      //.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      // 3) Pruning Step
      .filter(_._2 >= minSup)
  }



  private def containsAllFromPreRule(newRule: String, preRule: String): Boolean = {

    println("NEW" + newRule)

    // TODO do this some other way
    val newRuleCleaned = newRule.replaceAll("\\s+", "").replaceAll("[\\[\\](){}]", "")
    val preRuleCleaned = preRule.replaceAll("\\s+", "").replaceAll("[\\[\\](){}]", "")
    //println(newRule + " " + preRule)

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
      System.err.println("Usage: AssociationRule <input path> <result path> <iterationCount> <minSupport> <isFamily>")
      false
    }
  }
}
class AssociationRuleMining {

}

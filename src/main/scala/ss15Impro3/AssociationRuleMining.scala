package ss15Impro3

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object AssociationRuleMining {

  // example for cli params: inputPath outputPath
  // "/Software/Workspace/useCaseZ1/input/250data.txt" "/Software/Workspace/useCaseZ1/output" "/Software/Workspace/useCaseZ1/output/prepOutput"
   var inputFilePath: String = ""
   var outputFilePath: String = ""
   var prepOutputPath = ""

   //var inputFilePath: String = "/home/jjoon/250data.txt"
   //var outputFilePath: String = "/home/jjoon/output/"
   //var prepOutputPath = "/home/jjoon/output/"

  private var maxIterations: String = "6"
  private var minSupport: String = "3"

  // Test Case fileInput = false
  //private val fileInput: Boolean = false
  private val parseContents = " "
  //private val parseKeyValue = "\t"

  def main(args: Array[String]) {


    if (args.length < 3) {
      sys.error("inputFilePath, outputPath and prepOutputPath console parameters are missing")
      sys.exit(1)
    }

    inputFilePath = args(0)
    outputFilePath = args(1)
    prepOutputPath = args(2)
    println("inputFilePath: " + inputFilePath)
    println("outputFilePath: " + inputFilePath)

//    if (!parseParameters(args)) {
//      return
//    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val salesData: DataSet[String] = env.readTextFile(inputFilePath)
    val salesFilterData = salesData.filter(_.contains("SALE"))

    val salesOnly = salesFilterData
      //TODO change 1 to 2; or just leave it; TTYL
      .map(t => (t.split("\\s+")(1), t.split("\\s+")(3).replace(",", " ")
      // Remove Prefix "p-" to apply real dataset to our mining algorithm
      .replace("p-", "")))
      // Group by user session
      .distinct
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + " " + t2._2))
      .map(t => t._2)

    // Collect Real Dataset according to User_session and SALE
    salesOnly.writeAsText(prepOutputPath + "/sales", WriteMode.OVERWRITE)

    // Run our algorithm with the sales REAL DATA
    run(salesOnly, outputFilePath, maxIterations.toInt, minSupport.toInt)

    // Previous work for TESTING
    //val text = getTextDataSet(env)
    // 0) FrequentItem Function
    //val input = parseText(text)
    //run(input, outputFilePath, maxIterations.toInt, minSupport.toInt)

    env.execute("Scala AssociationRule Example")
  }

  private def run(parsedInput: DataSet[String], output: String, maxIterations: Int, minSup: Int): Unit = {
    var kTemp = 1
    var hasConverged = false
    val emptyArray: Array[Tuple2[String, Int]] = new Array[(String, Int)](0)
    val emptyDS = ExecutionEnvironment.getExecutionEnvironment.fromCollection(emptyArray)
    var preRules: DataSet[Tuple2[String, Int]] = emptyDS

    // According to how much K steps are, Making Pruned Candidate Set
    while (kTemp < maxIterations && !hasConverged) {
      println()
      printf("Starting K-Path %s\n", kTemp)
      println()

      val candidateRules: DataSet[Tuple2[String, Int]] = findCandidates(parsedInput, preRules, kTemp, minSup)

      val tempRulesNew = candidateRules
      // TODO Is it ok to collect here?
      val cntRules = candidateRules.collect.length

      if (kTemp >= 2) {

        // TODO Change it with some kind of join with special function
        val confidences: DataSet[Tuple2[String, Double]] = preRules
          .crossWithHuge(tempRulesNew)
          .filter { item => containsAllFromPreRule(item._2._1, item._1._1) }
          .map(
            input =>
              Tuple2(input._1._1 + " => " + input._2._1, 100 * (input._2._2 / input._1._2.toDouble))
            //RULE: [2, 6] => [2, 4, 6] CONF RATE: 4/6=66.66
          )
        // TODO Should this be here ot in the main function?
        confidences.writeAsText(outputFilePath + "/" + kTemp, WriteMode.OVERWRITE)
      }

      if (0 == cntRules) {
        hasConverged = true
      } else {

        preRules = candidateRules

        kTemp += 1
      }
    }

    printf("Converged K-Path %s\n", kTemp)
  }

  def findCandidates(candidateInput: DataSet[String], prevRulesNew: DataSet[Tuple2[String, Int]], k: Int, minSup: Int): DataSet[Tuple2[String, Int]] = {

    // 1) Generating Candidate Set Depending on K Path
    candidateInput.flatMap(

      new RichFlatMapFunction[String, Tuple2[String, Int]]() {

        var broadcastedPreRules: util.List[(String, Int)] = null

        override def open(config: Configuration): Unit = {
          // 3. Access the broadcasted DataSet as a Collection
          broadcastedPreRules = getRuntimeContext().getBroadcastVariable[Tuple2[String, Int]]("prevRules")
        }

        def flatMap(in: String, out: Collector[Tuple2[String, Int]]) = {

          val cItem1: Array[Int] = in.split(parseContents).map(_.toInt).sorted

          val combGen1 = new CombinationGenerator()
          val combGen2 = new CombinationGenerator()

          var candidates = scala.collection.mutable.ListBuffer.empty[(String, Int)]
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

                // TODO If broadcast variable is bad solution then try this -> (BUT) Not serializable exception (THese should be the dataset solution)
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
              out.collect(Tuple2(java.util.Arrays.toString(cItem2), 1))
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

    // input, output maxIterations, kPath, minSupport
    if (args.length > 0) {
      if (args.length == 4) {
        inputFilePath = args(0)
        outputFilePath = args(1)
        maxIterations = args(2)
        minSupport = args(3)
        true
      } else {
        System.err.println("Usage: AssociationRule <input path> <result path>")
        false
      }
    } else {
      System.out.println("Executing AssociationRule example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  The dataset is from local variable, not from inputPath as parameter.")

      true
    }
  }

  // Previous work for TESTING
  /*
  private def getTextDataSet(env: ExecutionEnvironment): DataSet[String] = {

    if (fileInput) {
      println("From File")
      env.readTextFile(inputFilePath)
    }
    else {
      println("From Code")
      env.fromCollection(ss15Impro3.RecommendationData.ITEMS)
    }
  }

  // TESTING
  private def parseText(textInput:DataSet[String]) = {
    textInput.map { input =>
      input.split(parseContents).distinct.mkString(parseContents)
    }
  }
  */
}

class AssociationRuleMining {

}

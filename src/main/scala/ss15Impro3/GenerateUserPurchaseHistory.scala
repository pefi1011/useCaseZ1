package ss15Impro3

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector



object GenerateUserPurchaseHistory {

 // example for cli params: inputPath outputPath
 // "/home/vassil/workspace/useCaseZ1/input/preProcessed/sessionFamily/" "/home/vassil/workspace/useCaseZ1/output/userPurchaseHistory"

  var inputPath = ""
  var outputPath = ""


  def main(args: Array[String]) {

    if (args.length < 2) {
      sys.error("inputFilePath and outputPath console parameters are missing")
      sys.exit(1)
    }
    inputPath = args(0)
    outputPath = args(1)
    println("inputFilePath: " + inputPath)
    println("outputFilePath: " + outputPath)


    val env = ExecutionEnvironment.getExecutionEnvironment



    val transactionData: DataSet[(String,String,String,String,String)] = env.readCsvFile(inputPath)

    val combinedItemsForUser = transactionData
      .filter(_._5.equals("SALE"))
      .flatMap(

      new FlatMapFunction[(String,String,String,String,String), (String,String)] {

        def flatMap(in: (String,String,String,String,String), out: Collector[(String, String)]) = {

          val user = in._2
          val products: List[String] = in._4.split(";").toList

          for (product <- products) {
            out.collect((user, product.replace("p-", "")))
          }
        }
      }).distinct
      // user grouping
      .groupBy(0)
      .reduce((t1,t2) => (t1._1, t1._2 + " " + t2._2)) // To combine all bought products

    combinedItemsForUser.writeAsCsv(outputPath + "/userPurchaseHistory/allItemsOfUser" ,"\n", ",", WriteMode.OVERWRITE)

    /*
    val itemsForUser = transactionData
      .filter(_._5.equals("SALE"))
      .map(t => (t._2, t._4))
      .groupBy(0)
      .reduce((t1,t2) => (t1._1, t1._2 + " " + t1._2) )

    itemsForUser.writeAsCsv(outputPath + "/userPurchaseHistory/singleTransactions" ,"\n", ",", WriteMode.OVERWRITE)
    */
    env.execute("Scala AssociationRule Example")
  }


}

class GenerateUserPurchaseHistory {

}

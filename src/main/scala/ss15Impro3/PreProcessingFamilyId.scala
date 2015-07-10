package ss15Impro3

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector



object PreProcessingFamilyId {

  // example for cli params: inputPath outputPath
  // "/Software/Workspace/useCaseZ1/input/datzal.txt" "/Software/Workspace/useCaseZ1/output"
  var inputPath = ""
  var outputPath = ""


  def main(args: Array[String]) {
    //TODO

    if (args.length < 2) {
      sys.error("inputFilePath and outputPath console parameters are missing")
      sys.exit(1)
    }
    inputPath = args(0)
    outputPath = args(1)
    println("inputFilePath: " + inputPath)
    println("outputFilePath: " + outputPath)


    val env = ExecutionEnvironment.getExecutionEnvironment



    val userData: DataSet[String] = env.readTextFile(inputPath)

    val userFilterData = userData
      .filter(_.contains("SALE"))
      .flatMap(

        new FlatMapFunction[String, (String,String)] {

          def flatMap(in: String, out: Collector[Tuple2[String, String]]) = {

            val info: Array[String] = in.split("\t")

            val user = info(1)
            val products: List[String] = info(3).split(",").toList

            for (product <- products) {
              out.collect((user, product))

            }
          }
        }).distinct
      // user grouping
      .groupBy(0)
      .reduce((t1,t2) => (t1._1, t1._2 + " " + t2._2)) // To combine all bought products

    userFilterData.writeAsText(outputPath + "/userPurchaseHistory" , WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }


}

class PreProcessingFamilyId {

}

package ss15Impro3

import com.sun.jersey.json.impl.ImplMessages
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.MapFunction



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

    val productData: DataSet[String] = env.readTextFile(inputPath + "datainfo.txt")

    val productIdToProductFamily = productData.map(
      new MapFunction[String, (String, String)]() {

        def map(in: String): (String, String) = {

          val infos = in.split(";")

          // Remove the hours minutes and seconds of the date
          (infos(0), infos(1))
        }
      }
    )
    productIdToProductFamily.writeAsText(outputPath + "/pidToFid", WriteMode.OVERWRITE)

    val userData: DataSet[String] = env.readTextFile(inputPath + "50data.txt")

    val userFilterData = userData
      .flatMap(

        new FlatMapFunction[String, (String, String, String, String, String)] {

          def flatMap(in: String, out: Collector[(String, String, String, String, String)]) = {

            val info: Array[String] = in.split("\t")

            val user = info(1)
            val session = info(2)
            val products = info(3)
            val productsAsArray: Array[String] = info(3).split(",")
            val transactionType = info(4)

            for (prod <- productsAsArray) {
              out.collect((user, session, products.replace(",", ";"), transactionType, prod))

            }
          }
        }).distinct
      // Join with the product info data set
      .joinWithHuge(productIdToProductFamily).where(4).equalTo(0)
      // Get the product family information instead of product id
      .map(t => (t._1._1, t._1._2, t._1._3, t._1._4, t._2._2))
      //combine again the products which were bought in one transaction
      .groupBy(0,1,2,3)
      .reduce((t1, t2) => (t1._1, t1._2, t1._3, t1._4, t1._5 + ";" + t2._5))
      // family ids instead
      .map(t => t._5)

    userFilterData.writeAsText(outputPath + "/preProcessingFamilyId" , WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }


}

class PreProcessingFamilyId {

}

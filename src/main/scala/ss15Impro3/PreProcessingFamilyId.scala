package ss15Impro3

import org.apache.flink.api.common.functions.{ReduceFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


object PreProcessingFamilyId {

  // example for cli params: inputPath outputPath
  var inputPathProduct = ""
  var inputPathShop = ""
  var outputPath = ""


  def main(args: Array[String]) {

    if (args.length < 3) {
      sys.error("inputFile_Product and inputFile_Shop and outputPath console parameters are missing")
      sys.exit(1)
    }

    inputPathProduct = args(0)
    inputPathShop = args(1)
    outputPath = args(2)
    println("inputFilePath_product: " + inputPathProduct)
    println("inputFilePath_shop: " + inputPathShop)
    println("outputFilePath: " + outputPath)


    val env = ExecutionEnvironment.getExecutionEnvironment


    // Read and process the product information
    val productData: DataSet[String] = env.readTextFile(inputPathProduct)

    val productIdToProductFamily = productData.map(
      new MapFunction[String, (String, String)]() {

        def map(in: String): (String, String) = {

          val infos = in.split(";")

          // Remove the hours minutes and seconds of the date
          (infos(0), infos(1).replace(" ",""))
        }
      }
    )

    // Read an process the transactions
    val userData: DataSet[String] = env.readTextFile(inputPathShop)
    val userFilterData = userData
      .flatMap(

        new FlatMapFunction[String, (String, String, String, String, String, String)] {

          def flatMap(in: String, out: Collector[(String, String, String, String, String, String)]) = {

            val info: Array[String] = in.split("\t")

            val day = info(0)
            val user = info(1)
            val session = info(2)
            val products = info(3)
            val productsAsArray: Array[String] = info(3).split(",")
            val transactionType = info(4)

            for (prod <- productsAsArray) {
              out.collect((day, user, session, products.replace(",", ";"), transactionType, prod))

            }
          }
        }).distinct
      // Join with the product info data set
      .joinWithHuge(productIdToProductFamily).where(5).equalTo(0)
      // Get the product family information instead of product id
      .map(t => (t._1._1, t._1._2, t._1._3, t._1._4, t._1._5, t._2._2))
      //combine again the products which were bought in one transaction

      .groupBy(0,1,2,3,4)
      // family ids instead of product id
      .reduce((t1, t2) => (t1._1, t1._2, t1._3, t1._4, t1._5, t1._6 + ";" + t2._6))
      //d-0,u-1003084,u-1003084_0,f-941055,SALE
      .map(t => (t._1, t._2, t._3, t._6, t._5))

    // join productId or familyId according to the session
    val userSession = userFilterData
      .groupBy(2)
      .reduce(new ReduceFunction[(String, String, String, String, String)] {
      override def reduce(t1: (String, String, String, String, String), t2: (String, String, String, String, String)): (String, String, String, String, String) = {

        var ids = t1._4
        for (prv <- t2._4.split(";")) {
          if (!ids.contains(prv)) {
            ids = ids +";" + prv
          }
        }

        return (t1._1, t1._2, t1._3, ids, t1._5)
      }
    })

    userSession.writeAsCsv(outputPath, "\n", ",", WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }

}

class PreProcessingFamilyId {

}

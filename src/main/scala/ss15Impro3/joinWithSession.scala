package ss15Impro3

import org.apache.flink.api.common.functions.{ReduceFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


object joinWithSession {

  var inputPathProduct = ""
  var outputPath = ""

  def main(args: Array[String]): Unit = {

    inputPathProduct = args(0)
    outputPath = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val userFilterData: DataSet[(String, String, String, String, String)] = env.readCsvFile(inputPathProduct)


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

class joinWithSession {

}

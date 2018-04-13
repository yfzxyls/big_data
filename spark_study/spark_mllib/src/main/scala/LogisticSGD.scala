import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.{SparkConf, SparkContext}

object LogisticSGD {
  def parseLine(line: String): LabeledPoint = {
    val parts = line.split("\t")
    val vd: Vector = Vectors.dense(parts(1).toDouble, parts(2).toDouble, parts(3).toDouble)
    LabeledPoint(parts(0).toDouble, vd)
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("LogisticSGD")
    val sc = new SparkContext(conf)
    val data = sc.textFile("D:\\study\\big_data\\spark_study\\spark_mllib\\src\\main\\resources\\traffic.csv").map(parseLine(_))

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainData = splits(0)
    val testData = splits(1)

//    LogisticRegressionModel.
    val model = LogisticRegressionWithSGD.train(trainData, 50)

    println(model.weights.size)
    println(model.weights)
    println(model.weights.toArray.filter(_ != 0).size)

    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))

    predictionAndLabel.foreach(println)

  }
}

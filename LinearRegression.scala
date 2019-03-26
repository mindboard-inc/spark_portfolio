package LinRegression

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors

object Test {
	def main(args: Array[String]): Unit = {

		case class Vals(cement:Double,slag:Double,flyash:Double,water:Double,
			superplasticizer:Double,coarseaggregate:Double,fineaggregate:Double,age:Double,csMPa:Double)

		val dataRDD = sc.textFile("data\\Concrete_Data.csv")
		val dataSplitRDD = dataRDD.map(line => line.split(","))
		val mappedDataRDD = dataSplitRDD.map(p => Vals(p(0).toDouble,p(1).toDouble,p(2).toDouble,p(3).toDouble,p(4).toDouble,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble))
		val dataFrame = mappedDataRDD.toDF
		val dataForModel = dataFrame.select(dataFrame("csMPa").as("label"),$"cement",$"slag",$"flyash",$"water",$"superplasticizer",$"coarseaggregate",$"fineaggregate",$"age")

		val assembler = new VectorAssembler().setInputCols(Array("cement","slag","flyash","water","superplasticizer","coarseaggregate","fineaggregate","age")).setOutputCol("features")
		//assembler: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_4c5ea5e20741
		val DF = assembler.transform(dataForModel).select($"label",$"features")

		val linReg = new LinearRegression()

		val linRegModel = linReg.fit(DF)

		println(s"Coefficients: ${linRegModel.coefficients} Intercept: ${linRegModel.intercept}")

		val trainingSummary = linRegModel.summary

		println(s"numIterations: ${trainingSummary.totalIterations}")
		println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

		trainingSummary.residuals.show()

		println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
		println(s"MSE: ${trainingSummary.meanSquaredError}")
		println(s"r2: ${trainingSummary.r2}")
	}
}
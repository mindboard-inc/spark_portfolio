package MultilayerPerceptronClassifier

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}


object Test {
	def main(args: Array[String]): Unit = {

		//feature columns labels
		val featureCols = (for (i <- List.range(1, 29)) yield (
			for (j <- List.range(1, 29)) yield i + "x" + j)).flatten.toArray
		//define schema structure
		val schemaStruct = StructType(
		            StructField("label", DoubleType) :: (for (i <- List.range(0, (28*28))) yield StructField(featureCols(i),
		            	DoubleType))
		        )
		//load training data
		val trainingData = spark.read.option("header", true).schema(schemaStruct).csv("data\\mnist_train.csv")
		//load test data
		val testData = spark.read.option("header", true).schema(schemaStruct).csv("data\\mnist_test.csv")
		//assemble feature vector
		val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
		val train = assembler.transform(trainingData).select("label","features")
		val test = assembler.transform(testData).select("label","features")
		//define network layout
		val layers = Array[Int](784, 1028, 1028, 10)
		//buuld model
		val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)
		//fit model
		val model = trainer.fit(train)
		//get test results
		val result = model.transform(test)
		//get accuracy
		val predictionAndLabels = result.select("prediction", "label")
		val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
		println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
	}
}
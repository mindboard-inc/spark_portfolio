package home_credit

object Test {
	def main(args: Array[String]): Unit = {
		import spark.implicits._
		///List data files
		import java.io.File
		def getListOfFiles(dir: String):List[File] = {
			val d = new File(dir)
			if (d.exists && d.isDirectory) {
		  		d.listFiles.filter(_.isFile).toList
			} else {
		  		List[File]()
			}
		}
		val files = getListOfFiles("c:\\data\\home_credit")
		//Load training data
		val app_train = spark.read.option("header", true).csv("c:\\data\\home_credit\\application_train.csv")
		app_train.show()
		//Load test data
		val app_test = spark.read.option("header", true).csv("c:\\data\\home_credit\\application_test.csv")
		app_test.show()
		//Count number of loans falling into each target category
		val grouped_targets = app_train.groupBy("TARGET")
		val total_targets = grouped_targets.count()
		total_targets.show()
		//Function for getting missing value information
		def missing_values_table(df:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame = {
			//Count # of Nulls in each column
			val nullCounts = for (colName <- df.columns) yield df.filter(df(colName).isNull || df(colName) === "" || df(colName).isNaN).count()
			// Pair count with column names
			val namedNullCounts = (for {a <- df.columns; b <- nullCounts} yield (a,b))
			//Compute number of Nulls as percentage of total rows
			val namedNullPercent = (for (a <- namedNullCounts) yield (a._1,a._2,100 * a._2 / df.count())).toList
			//Create DF displaying results for each column
			val col_names = List("Feature","Missing Values","Percent of Total Values")
			val missingValDF = namedNullPercent.toDF(col_names: _*)
			val orderedMissingValDF = missingValDF.orderBy(desc("Percent of Total Values"))
			//Return DF
			orderedMissingValDF
		}
		val missing_values = missing_values_table(app_train)
		missing_values.show()
		//Function for getting missing unique entries information
		def unique_entries_table(df:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame = {
			//Count # of unique values in each column
			val uniqueEntries = (for (colName <- df.columns) yield (colName,df.groupBy(colName).count().count())).toList
			//Create DF displaying results for each column
			val col_names = List("Feature","Number of Unique Values")
			val uniqueEntriesDF = namedNullPercent.toDF(col_names: _*)
			val orderedUniqueEntriesDF = missingValDF.orderBy(desc("Number of Unique Values"))
			//Return DF
			orderedUniqueEntriesDF
		}
		val unique_entries = unique_entries_table(app_train)
		unique_entries.show()
	}
}

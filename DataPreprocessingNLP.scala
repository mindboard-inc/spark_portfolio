import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.NGram


object Test {
  def main(args: Array[String]): Unit = {

    //read news article data into dataframe csv
    val data = spark
        .read.option("header", true)
        .csv("data\\articles.csv")

    //tokenizer is set to read "content" column and transform into individual words in the "words" column
    val tokenizer = new Tokenizer()
        .setInputCol("content")
        .setOutputCol("words")

    //use tokenziser to create new column of tokenized strings
    val tokenized_data = tokenizer
        .transform(data)

    //StopWordsRemover eliminates non essential tokens from vocaublary
    val remover = new StopWordsRemover()
        .setInputCol("words").setOutputCol("filtered")

    //use StopWordsRemover to create new column of essential strings
    val removed_data = remover
        .transform(tokenized_data)

    //Word2Vec learns a vector encoding for each token that efficiently represents the vocabulary
    val word2Vec = new Word2Vec()
        .setInputCol("filtered")
        .setOutputCol("vec")
        .setVectorSize(3)
        .setMinCount(0)

    //creat model object for learning Word2Vec encoding
    val model = word2Vec
        .fit(removed_data)

    //use Word2Vec model to create new column of vectorized strings
    val vectorized_data = model
        .transform(removed_data)

    //compute correlations between vectorized tokens
    val Row(coeff1: Matrix) = Correlation
        .corr(vectorized_data, "vec").head
    println(s"Pearson correlation matrix:\n $coeff1")

    //create ngram object for generating 3-grams of tokens
    val ngram = new NGram()
        .setN(3)
        .setInputCol("filtered")
        .setOutputCol("ngrams")

    //use ngram object to add 3-grams tokens as a column
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)

  }
}
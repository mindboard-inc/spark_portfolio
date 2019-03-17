package TestPackage

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    var df = spark.read.format("csv").option("header", "true").load("C:\\data\\20190307_20190308.csv")
    df = df.drop("TICKCOUNT","OBV","CV_BID","CV_ASK")
    df = df.toDF("DateTimeStamp","Price","Bid_Volume","Ask_Volume","Depth_Bid","Skewness_Bid",
    	"Kurtosis_Bid","Depth_Ask","Skewness_Bid","Kurtosis_Ask")
  	df = df.withColumn("TotalMarketDepth", df("Depth_Bid") + df("Depth_Ask")).
  		withColumn("BidAskRatio", df("Depth_Bid") / df("Depth_Ask")).
  		withColumn("AskBidRatio", df("Depth_Ask") / df("Depth_Bid"))
	def policy(qvalues: List[Double], position: Int) = {
		val action_index = qvalues.zipWithIndex.maxBy(_._1)._2
		val qval = qvalues.max
		var action = 0
		if (position == 0) (
			if (qval >= 0) action=action_index
			else action=2
		)
		else (if (position == 1) (
			if (action_index == 0) action = 0
			else action = 2
		)
		else (
			if (action_index == 0) action = 1
			else action = 2
		))
		action
	}
	def reward(price: Double, new_price:Double, position:Int, new_position:Int) = {
		var profit = 0.0
		if ((position == 0) & (new_position == 2)) 
			profit = ((new_price - price) * 1000) - 26.03
		else (if ((position == 0) & (new_position == 1)) 
			profit = ((price - new_price) * 1000) - 26.03
		else (if ((position == 1) & (new_position == 1)) 
			profit = ((price - new_price) * 1000)
		else (if ((position == 2) & (new_position == 2))
			profit = ((new_price - price) * 1000)
		else (if ((position == 1) & (new_position == 0))
			profit = 0
		else (if ((position == 2) & (new_position == 0))
			profit = 0
		else
			profit = 0
		)))))
		profit
	}
	def get_position(position: Int, action: Int) {
		var new_pos = 0
		if (action == 0) (
			if (position == 0) 
				new_pos = 2
			else (if (position == 1)
				new_pos = 0
			)
		)
		else (if (action == 1) (
			if (position == 0)
				new_pos = 1
			else (if (position == 2)
				new_pos = 0
			)
		)
		else new_pos = position
		)
		new_pos
	}
	for(i <- 0 to 6) {
		df = df.withColumn("reward" + i.toInt,lit(0.0))
		df = df.withColumn("q" + i.toInt,lit(0.0))
	}
	def add_rewards(df:org.apache.spark.sql.DataFrame,unscaled_prices: List[Double]) {
    	var all_rewards = List()
    	var reward_vals = List()
    	var new_position = 0
    	val position_action = Array(List(0,1,2),List(0,2),List(1,2))
   		for(n <- 0 to (df.count() - 1).toInt) {
   			reward_vals = List()
   			for(position: Int <- 0 to position_action.length) {
   				for(action_index <- 0 to position_action(position).length - 1) {
   					new_position = get_position(position, position_action(position)(action_index))
   					reward_vals = reward_vals :+ reward(unscaled_prices(n),unscaled_prices(n+1),position,new_position)	
   				}
   			all_rewards :+ reward_vals	
   			}
   		for(q <- 0 to 6) {
   		}
   		}
   	}
   	def get_qvalues(df:org.apache.spark.sql.DataFrame) {
   		for(itr <- 0 to (df.count()-1).toInt) {
   		}
   	}
	}
  }
}
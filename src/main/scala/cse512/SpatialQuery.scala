//Group 17 Phase 1
package cse512

import org.apache.spark.sql.SparkSession
import scala.math.sqrt
import scala.math.pow

object SpatialQuery extends App{
def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {

    if (pointString1 == null || pointString1.isEmpty() || pointString2 == null || pointString2.isEmpty() || distance <= 0.00)
        return false //null checks
	
    val point1_coordinates = pointString1.split(",") //getting coordinates of first point
    var point1x = point1_coordinates(0).toDouble
    var point1y = point1_coordinates(1).toDouble

    val point2_coordinates = pointString2.split(",") //getting coordinates of second point
    var point2x = point2_coordinates(0).toDouble
    var point2y = point2_coordinates(1).toDouble

    // getting the distance between these points
    var euc_dist = sqrt(pow(point1x - point2x, 2) + pow(point1y - point2y, 2))
    if (euc_dist <= distance)
        return true
    else
        return false
	}
def ST_Contains(queryRectangle:String, pointString:String):Boolean={
    val point = pointString.split(',').map(_.toDouble)
    val rectangle = queryRectangle.split(',').map(_.toDouble)

    val upper_bound_x = math.max(rectangle(0), rectangle(2))
    val lower_bound_x = math.min(rectangle(0), rectangle(2))
    val upper_bound_y = math.max(rectangle(1), rectangle(3))
    val lower_bound_y = math.min(rectangle(1), rectangle(3))

    val point_x = point(0)
    val point_y = point(1)

    if (point_x > upper_bound_x || point_x < lower_bound_x || point_y > upper_bound_y || point_y < lower_bound_y) return false
    else return true
  }
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - DONE
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - DONE
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> ST_Contains(queryRectangle, pointString))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}

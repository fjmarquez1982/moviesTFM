package es.batch.ml.recomended

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

/**
  *
  */
object UserMovieRecomended {


  var movies: DataFrame = null
  var valorations:DataFrame = null
  var userId: Int = 0
  var personalRatings: DataFrame = null

  /**
    * Entrenamos el model con las peliculas valoradas por un usuario
    * @return
    */
  def train(sqlContext: SQLContext): MatrixFactorizationModel ={

    import sqlContext.implicits._

    //Splitting training data 90% for training & 10% for testing
    val set = valorations.randomSplit(Array(0.9, 0.1), seed = 12345)
    val training = personalRatings.unionAll(set(0)).cache()
    val test = set(1).cache()

    //println(s"Training: ${training.count()}, test: ${test.count()}")

    val trainRDD = training.as[Rating].rdd
    val rank = 20
    val numIterations = 10

    //Training the recommendation model using ALS
    ALS.train(trainRDD, rank, numIterations, 0.01)

  }


  /**
    * Para un usuario, recomendamos las peliculas
    * @param userId
    * @param model
    * @return
    */
  def predict(sqlContext: SQLContext,userId: Int, model : MatrixFactorizationModel): Dataset[Rating] = {
    import sqlContext.implicits._

    val usersProducts = movies.select(lit(userId), col("movieId")).map{
      row => (row.getInt(0), row.getInt(1))
    }

    model.predict(usersProducts.toJavaRDD).toDS()

  }

}

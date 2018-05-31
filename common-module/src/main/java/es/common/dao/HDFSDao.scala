package es.common.dao

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  *
  */
class HDFSDao {

  val hdfsUrl = "hdfs://localhost:9000";

  /**
    *
    * @param dataFrame
    * @param path
    */
  def saveToHdfs(dataFrame: DataFrame, path: String): Unit = {
    var pathToSave = hdfsUrl + path
    println("guardando  "+dataFrame+ " en "+ pathToSave)
    dataFrame.write.mode(SaveMode.Append).parquet(pathToSave)
  }

}

package climate.op

import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.spark._
import org.apache.spark.rdd.PairRDDFunctions
import geotrellis.raster.op.stats._
import geotrellis.raster.histogram._

import scala.reflect.ClassTag


object BinSum {
  def apply[K: ClassTag](cellType: CellType, keyBin: K => K)(rdd: RasterRDD[K]): RasterRDD[K] =
    rdd
      .mapKeys { keyBin }
      .convert(cellType)
      .reduceByKey { (t1, t2) => t1.localAdd(t2)}
  }

object Histogram {
  def apply[K: ClassTag](rdd: RasterRDD[K]): Histogram = {
    rdd
      .map{ case (key, tile) => tile.histogram }
      .reduce { (h1, h2) => FastMapHistogram.fromHistograms(Array(h1,h2)) }
  }
}

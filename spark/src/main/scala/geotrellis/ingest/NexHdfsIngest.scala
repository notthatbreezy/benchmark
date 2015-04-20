package climate.ingest

import geotrellis.spark._
import geotrellis.spark.ingest.NetCDFIngestCommand._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.s3._
import geotrellis.spark.ingest._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.utils.SparkUtils
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat
import org.apache.accumulo.core.data.{ Value, Key, Mutation }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark._
import com.quantifind.sumac.ArgMain
import com.github.nscala_time.time.Imports._
import com.typesafe.scalalogging.slf4j.Logging

/** Ingests the chunked NEX GeoTIFF data */
object NexHdfsIngest extends ArgMain[HadoopIngestArgs] with Logging {
  def main(args: HadoopIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")
    val job = sparkContext.newJob

    val layoutScheme = ZoomedLayoutScheme()

    val inPath = args.inPath
    S3InputFormat.setUrl(job, inPath.toUri.toString)

    val source = 
      sparkContext.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TemporalGeoTiffS3InputFormat],
        classOf[SpaceTimeInputKey],
        classOf[Tile]
      )
  
    val save = { (rdd: RasterRDD[SpaceTimeKey], level: LayoutLevel) =>
      val layer = LayerId(args.layerName, level.zoom)
      val outPath = s"${args.catalog}/${args.layerName}/${level.zoom}"
      val encoded = 
        rdd
        .map { case (key, tile) => 
          TimeRasterAccumuloDriver.getKey(layer, key) -> new Value(tile.toBytes)
        }
        .sortBy(_._1)
        
      import org.apache.spark.SparkContext._
      encoded.saveAsNewAPIHadoopFile(outPath, classOf[Text], classOf[Mutation], classOf[AccumuloFileOutputFormat], job.getConfiguration)
    }

    val (level, rdd) =  Ingest[SpaceTimeInputKey, SpaceTimeKey](source, args.destCrs, layoutScheme)

    if (args.pyramid) {
      Pyramid.saveLevels(rdd, level, layoutScheme)(save)
    } else{
      save(rdd, level)
    }
  }
}

package org.template.similarproduct

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.storage.Storage
//@sagar_start
import collection.immutable.ListMap
import org.apache.mahout.math.DenseVector
import org.apache.mahout.math.drm._
import scala.collection.Seq
import scala.collection.GenSeq
import scala.collection.mutable.ArrayBuffer
import io.prediction.data.storage.BiMap
import com.google.common.collect.HashBiMap
import scala.util.control._
import org.apache.mahout.math.drm.CheckpointedDrm
import org.apache.mahout.sparkbindings
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.mahout.math.indexeddataset.IndexedDataset
import org.apache.mahout.sparkbindings.drm.CheckpointedDrmSpark
import org.apache.mahout.math.Vector
import org.apache.spark.mllib.linalg.Vectors
import collection.JavaConverters._
//@sagar_stop
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val eventsDb = Storage.getPEvents()

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = eventsDb.aggregateProperties(
      appId = dsp.appId,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = eventsDb.aggregateProperties(
      appId = dsp.appId,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    // get all "user" "view" "item" events
    val viewEventsRDD: RDD[ViewEvent] = eventsDb.find(
      appId = dsp.appId,
      entityType = Some("user"),
      eventNames = Some(List("view")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      // eventsDb.find() returns RDD[Event]
      .map { event =>
        val viewEvent = try {
          event.event match {
            case "view" => ViewEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis)
            case _ => throw new Exception(s"Unexpected event ${event} is read.")
          }
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
          }
        }
        viewEvent
      }.cache()

//@sagar_start
    var index = -1;
    var users: Map[String, Int]  = usersRDD.map { case (id, user) =>
      (id, {index = index + 1; index})
    }.collectAsMap.toMap
    index = -1;
    var items: Map[String, Int]  = itemsRDD.map { case (id, item) =>
      (id, {index = index + 1; index})
    }.collectAsMap.toMap
    val usersG = HashBiMap.create[String, Int]()
    val itemsG = HashBiMap.create[String, Int]()
    for((k,v) <- users)
    	usersG.put(k,v)
    for((k,v) <- items)
    	itemsG.put(k,v)
    users = ListMap(users.toList.sortBy{_._2}:_*)
    items = ListMap(items.toList.sortBy{_._2}:_*)
    var flag = 0;
    var Outside = 0;
    var Inside = 0;
    var seq: ArrayBuffer[DrmTuple[Int]] = new ArrayBuffer[DrmTuple[Int]]()
    for((u, uId) <- users){
    	println(u, uId)
    	val userItemAffinity = new ArrayBuffer[Double]()
	val userItems:Map[String, Double] = viewEventsRDD.filter(vwEv => vwEv.user == u).map{vwEv => (vwEv.item, 1.0)}.collectAsMap.toMap
	for((i, iId) <- items){
		println(i, iId)
		if(userItems.keySet.contains(i)){
			userItemAffinity.append(1.0);
		}else{
			userItemAffinity.append(0.0);
		}	
	}
	println(userItemAffinity)
	seq.append((uId, (new DenseVector(userItemAffinity.toArray))))
    }
    print(seq)
    print("\n")



    val ind : IndexedDataset = new IndexedDatasetSpark(new CheckpointedDrmSpark[Int](sc.makeRDD[DrmTuple[Int]](seq)), usersG, itemsG)
//@sagar_stop

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD
    )
  }
}


case class User()

case class Item(categories: Option[List[String]])

case class ViewEvent(user: String, item: String, t: Long)

class TrainingData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }
}

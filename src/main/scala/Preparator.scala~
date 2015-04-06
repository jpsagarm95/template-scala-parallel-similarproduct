package org.template.similarproduct

import io.prediction.controller.PPreparator

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

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    //@sagar_start
    var index = -1;
    var users: Map[String, Int]  = trainingData.users.map { case (id, user) =>
      (id, {index = index + 1; index})
    }.collectAsMap.toMap
    index = -1;
    var items: Map[String, Int]  = trainingData.items.map { case (id, item) =>
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
	val userItems:Map[String, Double] = trainingData.viewEvents.filter(vwEv => vwEv.user == u).map{vwEv => (vwEv.item, 1.0)}.collectAsMap.toMap
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
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      viewEvents = trainingData.viewEvents,
//@sagar_start
      userItemMatrix = ind
//@sagar_stop
      )
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
//@sagar_start
  val userItemMatrix: IndexedDataset
//@sagar_stop
) extends Serializable

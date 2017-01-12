package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.pubsub.{ DistributedPubSubMediator, DistributedPubSub }
import cn.edu.tsinghua.ee.fi.odl.sim.util.MetricsMessages._
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitPhase


object MetricsActor {
  // transid, timestamp, commitphase
  type MutableMetricsContainer = collection.mutable.Buffer[(Int, Long, CommitPhase.CommitPhase, Long)]
  
  trait MetricsRecounter[T] {
    def recount(container: MutableMetricsContainer): MetricsResult[T]
  }
  
  def props[T](metricsRecounter: MetricsRecounter[T]): Props = Props(new MetricsActor[T](metricsRecounter))
}


class MetricsActor[T](metricsRecounter: MetricsActor.MetricsRecounter[T]) extends Actor with ActorLogging {
  
  import MetricsActor._
  import DistributedPubSubMediator.{Subscribe, SubscribeAck}
  
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("metrics", self)
  
  val metricsContainer: MutableMetricsContainer = collection.mutable.ListBuffer[(Int, Long, CommitPhase.CommitPhase, Long)]()
  
  override def receive = {
    case SubscribeAck(Subscribe("metrics", None, `self`)) =>
      log.info("metrics module subscribe successfully")
    case _: ReadyMetrics =>
      log.info("ready to meter")
      metricsContainer clear;
      sender ! ReadyMetricsReply(true)
    case FinishMetrics() =>
      sender ! FinishMetricsReply(metricsRecounter.recount(metricsContainer))
    case MetricsElement(transId, timestamp, process) =>
      val e = Tuple4(transId, timestamp, process, System.nanoTime())
      log.debug(s"element fetch: $e")
      metricsContainer += e
    case m @ _ =>
      log.debug(s"unhandled message $m")
  }
}


import MetricsActor.{MetricsRecounter, MutableMetricsContainer}


class EmptyRecounter extends MetricsRecounter[Any] {
  def recount(container: MutableMetricsContainer): MetricsResult[Any] = {
    null
  }
}


class TwoPhaseRecounter extends MetricsRecounter[Long] {
  import CommitPhase._
 
  def recount(container: MutableMetricsContainer): MetricsResult[Long] = {
    
    if (container.isEmpty) {
      null
    } else {
      // map1: transId -> { phase -> timestamp }
      val map1 = container groupBy(_._1) filter (_._2.size == 3) map (e => e._1 -> (e._2 map { t => t._3.phase -> t._2 } toMap) )
      // map2: transId -> { phase -> duration }
      val map2 = map1 map { e => 
        e._1 -> Map(
            "CanCommit"-> (e._2(PRE_COMMIT) - e._2(CAN_COMMIT)),
            "Commit" -> (e._2(COMMITED) - e._2(PRE_COMMIT))
            )
      }
      
      val resultPhase = ((map2.head._2 map { case (k, v) => k -> v / 1000 }) /: map2.tail) { (r, e) =>
        r map { case (k, v) => k -> (v + e._2(k) / 1000) }
      } map { case (k, v) => k -> (v / map2.size) }
      
      if (map2.size > 1) {
        /* Throughput meter. 
         * Set a timer t0 when first element arrives as well as another timer t1 that records time of the end element.
         * Se stands for sum of elements
         * Then Throughput shall be Se / (t1 - t0)
         */

        val submitCompleteDuration = (container.last._4 - container.head._4) / 1000000
        val submitCount = map2.size
        
        val resultThroughput = "Throughput" -> ((submitCount * 1000) / submitCompleteDuration)
        resultPhase + resultThroughput
      } else {
        resultPhase
      }
    }
  }
}


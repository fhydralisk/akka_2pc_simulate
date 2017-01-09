package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.pubsub.{ DistributedPubSubMediator, DistributedPubSub }
import cn.edu.tsinghua.ee.fi.odl.sim.util.MetricsMessages._


object MetricsActor {
  type MutableMetricsContainer = collection.mutable.Buffer[(Int, Long, CommitPhase.CommitPhase)]
  
  trait MetricsRecounter {
    def recount(container: MutableMetricsContainer): MetricsResult
  }
  
  def props(metricsRecounter: MetricsRecounter): Props = Props(new MetricsActor(metricsRecounter))
}


class MetricsActor(metricsRecounter: MetricsActor.MetricsRecounter) extends Actor with ActorLogging {
  
  // TODO: Implement this metrics actor to take the test meansurement
  import MetricsActor._
  import DistributedPubSubMediator.{Subscribe, SubscribeAck}
  
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe("metrics", self)
  
  val metricsContainer: MutableMetricsContainer = collection.mutable.ListBuffer[(Int, Long, CommitPhase.CommitPhase)]()
  
  override def receive = {
    case SubscribeAck(Subscribe("metrics", None, `self`)) =>
      log.info("metrics module subscribe successfully")
    case ReadyMetrics() =>
      metricsContainer clear;
      sender ! ReadyMetricsReply(true)
    case FinishMetrics() =>
      sender ! FinishMetricsReply(metricsRecounter.recount(metricsContainer))
    case MetricsElement(transId, timestamp, process) =>
      metricsContainer += Tuple3(transId, timestamp, process)
    case m @ _ =>
      log.debug(s"unhandled message $m")
  }
}
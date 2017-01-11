package cn.edu.tsinghua.ee.fi.odl.sim.nodes

import akka.actor.{Props, ActorLogging}
import com.typesafe.config.Config
import concurrent.duration._


object Operator {
  def props(submitConfig: Config): Props = Props(new Operator(submitConfig))
}


class Operator(submitConfig: Config) extends EndActor with ActorLogging {
  import cn.edu.tsinghua.ee.fi.odl.sim.util.MetricsMessages._
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages.DoSubmit
  
  import context.dispatcher
  context.system.scheduler.scheduleOnce(20 seconds) {
    log.info("trying to get metrics module ready")
    publish("metrics", ReadyMetrics())
  }
  
  lazy val metricsDuration = submitConfig.getDuration("metrics.duration")
  implicit def jDurationToSDuration: java.time.Duration => FiniteDuration = d => FiniteDuration(d.toNanos(), NANOSECONDS)
  
  def receive = {
    case ReadyMetricsReply(true) =>
      log.info(s"inform frontends to submit, metrics will be end in $metricsDuration")
      publish("dosubmit", DoSubmit(submitConfig.getConfig("submit")))
      context.system.scheduler.scheduleOnce(metricsDuration, sender, FinishMetrics())
    case FinishMetricsReply(metrics) =>
      log.info(s"metrics result is ${metrics}")
      
    case _ =>
  }
}
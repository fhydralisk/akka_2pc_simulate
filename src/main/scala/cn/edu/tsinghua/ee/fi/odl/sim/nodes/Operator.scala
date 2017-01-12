package cn.edu.tsinghua.ee.fi.odl.sim.nodes

import akka.actor.{Props, ActorLogging}
import com.typesafe.config.ConfigFactory
import concurrent.duration._


object Operator {
  def props: Props = Props(new Operator)
}


object OperatorConfiguration {
  val rootConfig = ConfigFactory.parseFile(new java.io.File("config/operator.conf"))
                                .withFallback(ConfigFactory.parseResources("operator.conf"))
                                .getConfig("testing")
    
  val submitConfig = rootConfig.getConfig("submit")
  val metricsConfig = rootConfig.getConfig("metrics")
}


class Operator extends EndActor with ActorLogging {
  import OperatorConfiguration._
  import cn.edu.tsinghua.ee.fi.odl.sim.util.MetricsMessages._
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages.DoSubmit
  
  import context.dispatcher
  
  val doCommitMessageDelay = rootConfig.getDuration("delay-test-after")
  val durationBetweenGroups = rootConfig.getDuration("duration-between-tests")
  val metricsDuration = metricsConfig.getDuration("duration")
  
  context.system.scheduler.schedule(doCommitMessageDelay, durationBetweenGroups + metricsDuration) {
    log.info("trying to get metrics module ready")
    publish("metrics", ReadyMetrics())
  }
  
  
  implicit def jDurationToSDuration: java.time.Duration => FiniteDuration = d => FiniteDuration(d.toNanos(), NANOSECONDS)
  
  def receive = {
    case ReadyMetricsReply(true) =>
      log.info(s"inform frontends to submit, metrics will be end in $metricsDuration")
      publish("dosubmit", DoSubmit(submitConfig))
      context.system.scheduler.scheduleOnce(metricsDuration, sender, FinishMetrics())
    case FinishMetricsReply(metrics) =>
      log.info(s"metrics result is ${metrics}")
      
    case _ =>
  }
}
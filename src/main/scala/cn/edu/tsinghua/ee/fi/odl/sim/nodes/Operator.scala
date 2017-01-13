package cn.edu.tsinghua.ee.fi.odl.sim.nodes

import akka.actor.{Props, ActorLogging}
import com.typesafe.config.Config
import concurrent.duration._


object Operator {
  def props(config: Config): Props = Props(new Operator(config))
}

/*
object OperatorConfiguration {
  val rootConfig = ConfigFactory.parseFile(new java.io.File("config/operator.conf"))
                                .withFallback(ConfigFactory.parseResources("operator.conf"))
                                .getConfig("testing")
    
  val submitConfig = rootConfig.getConfig("submit")
  val metricsConfig = rootConfig.getConfig("metrics")
}*/


class Operator(config: Config) extends EndActor with ActorLogging {
  import cn.edu.tsinghua.ee.fi.odl.sim.util.MetricsMessages._
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages.DoSubmit
  
  import context.dispatcher
  
  val metricsConfig = config.getConfig("metrics")
  val submitConfig = config.getConfig("submit")
  
  val doCommitMessageDelay = config.getDuration("delay-test-after")
  val durationBetweenGroups = config.getDuration("duration-between-tests")
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
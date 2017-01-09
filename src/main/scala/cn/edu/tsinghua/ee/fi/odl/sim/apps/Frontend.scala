package cn.edu.tsinghua.ee.fi.odl.sim.apps

import com.typesafe.config.ConfigFactory
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.DataBroker
import cn.edu.tsinghua.ee.fi.odl.sim.nodes.Frontend

/* TODO:
 * 1. Construct FakeBroker
 * 2. Implement TransactionIdGetter(? maybe Forward), get CohortProxyFactory from Leader
 * 3. Perform the test while Cluster is fully setup (how to be aware of this?)
 * 3.1 Get a new transaction from broker
 * 3.2 put some shard into the transaction
 * 3.3 submit the transaction
 * 3.4 metrics
 * TODO: metrics
 */
object FrontendApp {
  def main(args: Array[String]) {
    val frontendConfig = ConfigFactory.load("frontend.conf").withFallback(ConfigFactory.parseString("akka.cluster.roles=[\"frontend\"]"))
    val system = AkkaSystem.createSystem(Some(frontendConfig))
    system.actorOf(Frontend.props, name="frontend")
  }
}
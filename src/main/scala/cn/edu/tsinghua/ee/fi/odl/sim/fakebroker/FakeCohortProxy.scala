package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker


import concurrent.Future
import com.typesafe.config.Config

trait CohortProxy {
  def submit(): Future[Nothing]
}

class CohortProxyFactory(config: Config) {
  def getCohortProxy(txn: Transaction) : Option[CohortProxy] = {
    config.getString("cohort-proxy-type") match {
      case "SyncCohortProxy" =>
        Some(new SyncCohortProxy(txn))
      case "ForwardCohortProxy" =>
        Some(new ForwardCohortProxy(txn))
      case _ =>
        None
    }
  }
}

class SyncCohortProxy(txn: Transaction) extends CohortProxy {
  def submit(): Future[Nothing] = {
    
  }
}

class ForwardCohortProxy(txn: Transaction) extends CohortProxy {
  def submit() = {
    
  }
}
package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorLogging}


object Shard {
  
}

class Shard(canCommitQueue: SimplifiedQueue[AnyRef]) extends Actor with ActorLogging {
  //TODO: AnyRef -> Transaction
  def receive = {
    case CommitMessages.CanCommitMessage(txn) =>
      
    case _ : CommitMessages.CommitMessage =>
      sender() ! CommitMessages.CommitAck()
    case unknown : AnyRef => 
      log.warning("Message Not Found: " + unknown.toString())
  }
}
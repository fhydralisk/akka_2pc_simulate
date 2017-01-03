package cn.edu.tsinghua.ee.fi.odl.sim.util


import scala.collection.mutable.{Queue, TreeSet}


trait SimplifiedQueue[T] {
  def offer(node: T)
  def poll() : T
  def peek() : T
}

class QueueWrapper[T] extends SimplifiedQueue[T] {
  val backing = new Queue[T]()
  
  def offer(node : T) {
    backing.enqueue(node)
  }
  
  def poll() = backing.dequeue()
  
  def peek() = backing.front
}

class TreeSetWrapper[T](implicit ordering: Ordering[T]) extends SimplifiedQueue[T] {
  val backing = new TreeSet[T]()
}
package cn.edu.tsinghua.ee.fi.odl.sim.util


import scala.collection.mutable.{Queue, TreeSet}


trait SimplifiedQueue[T] {
  def offer(node: T)
  def poll() : Option[T]
  def peek() : Option[T]
  def removeWhen(f: T => Boolean): Option[T]
}

class QueueWrapper[T] extends SimplifiedQueue[T] {
  val backing = new Queue[T]()
  
  def offer(node : T) {
    backing.enqueue(node)
  }
  
  def poll() = if (backing.size > 0) Some(backing.dequeue()) else None
  
  def peek() = if (backing.size > 0) Some(backing.front) else None
  
  def removeWhen(f: T => Boolean) = backing dequeueFirst f
}

class TreeSetWrapper[T](implicit ordering: Ordering[T]) extends SimplifiedQueue[T] {
  val backing = new TreeSet[T]()
  
  def offer(node : T) {
    backing += node
  }
  
  def poll = {
    val e = backing.headOption
    // if e is None, then map will not be run and returns None
    e.map(f => {
      backing -= f
      f
    })
  }
  
  def peek = backing.headOption
  
  def removeWhen(f: T => Boolean) = {
    val e = backing.filter(f).headOption
    e foreach { backing.remove }
    e
  }
}

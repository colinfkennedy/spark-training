// Examine ways to create RDDs and manipulate how they are stored, etc.
import org.apache.spark.rdd.RDD
import scala.sys.process._
import java.io.File

// Helper to list the contents of a directory in a platform-independent way.
def listDir(file: File): Unit = {
  if (file.exists == false) {
    println(s"${file.getPath} doesn't exist!")
  } else if (file.isDirectory) {
    val list = file.listFiles
    if (list.size == 0) println(s"${file.getPath} is empty.")
    else list foreach listDir
  } else {
    println(file.getPath)
  }
}

// Parallelize converts a Scala collection to an RDD, where each collection
// element becomes a record in the RDD. I'm showing the explicit type annotation
// to emphasize what we get:
val ints: RDD[Int] = sc.parallelize(0 until 5)

// Give it a name, which will be useful below.
ints.name = "ints"

// Once we have an RDD, we can manipulate it in the usual way.
// Here I use each integer as a row index and generate vectors of tuples
// with (row,column) indices.
val tupleVects: RDD[Vector[(Int,Int)]] = ints.map {
  i => (0 until 6).map(j => (i,j)).toVector
}
tupleVects.name = "tupleVects"

// Show the lineage, which will be one RDD.
// dependencies returns a list of Dependency instances. We ask for each one's
// RDD and it's name. Note that a typical RDD will have only one ancestor.
// Joined data sets would have more.
tupleVects.dependencies map (_.rdd.name)

// Print them to be clear what we have:
tupleVects foreach println
// NOT SORTED, because we're using more than one core.
// Vector((0,0), (0,1), (0,2), (0,3), (0,4), (0,5))
// Vector((1,0), (1,1), (1,2), (1,3), (1,4), (1,5))
// Vector((2,0), (2,1), (2,2), (2,3), (2,4), (2,5))
// Vector((3,0), (3,1), (3,2), (3,3), (3,4), (3,5))
// Vector((4,0), (4,1), (4,2), (4,3), (4,4), (4,5))

val sums = tupleVects map { vect =>
  vect.foldLeft((0,0)) {
    case ((currentIndex, sum), (i, j)) => (i, sum + (i * 100) + j)
  }
}
sums.name = "sums"

// BEFORE WE DO ANY COMPUTATIONS OVER sums, checkpoint it.

// Ancestor RDDs: Should be non-empty
// dependencies returns a list of Dependency instances. We ask for each one's
// RDD and it's name. Note that each one will have one ancestor.
// Joined data sets would have more.
val sumDeps1 = sums.dependencies
println(s"sums.dependencies before checkpointing: # = ${sumDeps1.size}")
println(s"The dependencies names: ${sumDeps1 map (_.rdd.name)}")

val checkpointDir = "output/checkpoints"

sc.setCheckpointDir(checkpointDir)
sums.checkpoint
println("Checkpoint dir won't exist yet:")
val checkpointDirFile = new File(checkpointDir)
listDir(checkpointDirFile)

println(s"sums is checkpointed? ${sums.isCheckpointed}")
println(s"sums' checkpoint file: ${sums.getCheckpointFile}")

// Will force evaluation (will happen when we use foreach for printing), and
// hold in memory, spilling to disk if necessary. RDD lineage (how we got here)
// is not forgotten.
sums.cache

// Now look at its values.
sums foreach println
// Not sorted:
// (3,1815)
// (2,1215)
// (1,615)
// (4,2415)
// (0,15)

// Ancestor RDDs again: NOW the tupleVects is forgotten, and replaced with a
// a new ancestor, which is associated with the file storage.
val sumDeps2 = sums.dependencies
println(s"sums.dependencies after checkpointing: # = ${sumDeps2.size}")
println(s"The dependencies names: ${sumDeps2 map (_.rdd.name)}")

println("Checkpoint dir should now have at least one checkpoint dir and files:")
listDir(checkpointDirFile)

val partitions = sums.partitions
println(s" for ${partitions.size} partitions:")
partitions foreach { p =>
  println(s"Partition #${p.index}: preferred locations: ${sums.preferredLocations(p)}")
}

// You're only allowed to reassign a storage level from the default before
// computing over it. But calling with the default is okay:
// import org.apache.spark.storage.StorageLevel._
// sums.persist(MEMORY_AND_DISK)
sums.persist()

// Returns an obscure StorageLevel.toString value. Here's what the fields mean:
//   useDisk: Boolean
//   useMemory: Boolean
//   useOffHeap: Boolean
//   deserialized: Boolean
//   replication: Int
sums.getStorageLevel

// Change the number of partitions. The number can be greater than the
// original number, but a shuffle is performed to do this.
val sums8 = sums.repartition(8)
println(s"sums8 has ${sums8.partitions.size} partitions")

// Reduce the number of partitions. A shuffle is optional:
// sums.coalesce(2, true)
// Avoiding the shuffle is more efficient.
val sums2 = sums.coalesce(2)
println(s"sums2 has ${sums2.partitions.size} partitions")

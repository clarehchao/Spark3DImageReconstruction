import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object MLEMappv3{
  def BPSendMsg(triplet: EdgeContext[Double,Double,Double]){triplet.sendToDst(triplet.srcAttr * triplet.attr)}
  def FPSendMsg(triplet: EdgeContext[Double,Double,Double]){triplet.sendToSrc(triplet.dstAttr * triplet.attr)}
  def mergeMsg(a: Double, b: Double): Double = a + b
  def main(args: Array[String]) {

    // Configure Spark setup
    val conf = new SparkConf().setAppName("MLEM Application")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb","1000")
    val sc = new SparkContext(conf)

    // Initialize variables for 3D image reconstruction (MLEM)
    val numi = 128*128*360
    val numj = 128*128*128
    val numIterations = args(0).toInt
    val numPartitions = args(1).toInt
    val vLength = numi + numj - 1
    val epsilon = 1.0e-13
    val oneoverepsilon = 1.0e+13
    val rootdir = args(2)
    val Ntest = args(3)

    // the system matrix was written as sparse format => any sys mat value < 1.0e-14 was not stored
    val sm_fname = "%s/SystemMatrix2.csv".format(rootdir)

    // index < numj => image space, index > numj => sinogram space
    // edge relation: src [sinogram space] to dst [image space]
    val edges = sc.textFile(sm_fname,numPartitions).map(_.split(',')).map(l => Edge(l(0).toLong+numj, l(1).toLong, l(2).toDouble)).coalesce(numPartitions)

    // parallelize: the elements of the collection are copied to form a distributed database that can be operated in parallel
    val vertices = sc.parallelize(0 to vLength, numPartitions).map(x => (x.toLong,1.0))
    var theGraph = Graph(vertices, edges).cache()

    //importing the measured projection and # of partition should be set for optimized run (experiment with # of partitions for best uses)
    val sino_fname = "%s/projection_240_300x30.csv".format(rootdir)

    //filter sino_measured to be sparse
    val sino_measured = sc.textFile(sino_fname,numPartitions).map(_.split(',')).map(l => (l(0).toLong+numj, l(1).toDouble)).filter{ case (id,attr) => attr != 0.0}.coalesce(numPartitions).cache()

    // mapVertices: preserve the indices of graph ==> return a graph with (theGraph's indices and new attr)
    val init_sino = theGraph.mapVertices((vid, attr) => if (vid < numj) 0.0 else attr)
    val const_denom_vertices = init_sino.aggregateMessages(BPSendMsg,mergeMsg)
    const_denom_vertices.cache().count()
    val const_denom_graph_v = theGraph.outerJoinVertices(const_denom_vertices) { case(vid, a, Some(b)) => {if (vid < numj) {if (b.get < epsilon) oneoverepsilon else 1.0/b.get} else a} case(vid,a,None) => {if (vid < numj) oneoverepsilon else a}}.vertices.cache()

    // Make a template of the initial graph for correct update in the MLEM iteration
    // theGraph will be updated in the loop thus the vertices' attribute will be updated, which produces incorrect output if using theGraph for forward and backprojection
    var gtemplate = theGraph.cache()

    for(i <- 1 to numIterations){
      //forward projection (forward matrix multiplication)
      val fpGraph = theGraph.mapVertices((vid, attr) => if (vid < numj) attr else 0.0)
      val tmp_sino_vertices = fpGraph.aggregateMessages(FPSendMsg,mergeMsg)
      val tmp_sino_graph = gtemplate.outerJoinVertices(tmp_sino_vertices){ case(vid, a, Some(b)) => {if (vid < numj) a else a*b.get} case (vid, a, None) => {if (vid < numj) a else 0.0}}

      // get the ratio of sino_measured/tmp_sino
      val indx_vertices = tmp_sino_graph.vertices.aggregateUsingIndex(sino_measured, (_:Double) + (_:Double))
      val sino_ratio_graph = tmp_sino_graph.outerJoinVertices(indx_vertices){ case (vid, a, Some(b)) => {if (vid < numj) a else {if (a < epsilon) b.get*oneoverepsilon else b.get/a}} case (vid, a, None) => {if (vid < numj) a else 0.0}}


      // back-projection: zero out the image space
      val bpGraph = sino_ratio_graph.mapVertices((vid, attr) => if (vid < numj) 0.0 else attr)
      //backward projection: system matrix multiplication
      val tmp_vol_vertices = bpGraph.aggregateMessages(BPSendMsg,mergeMsg)
      val tmp_vol_graph = gtemplate.outerJoinVertices(tmp_vol_vertices){ case(vid, a, Some(b)) => if (vid < numj) a*b.get else a case (vid, a, None) => {if (vid < numj) 0.0 else a}}

      // update theGraph for another iteration to carry on correctly, sino space = all 0's
      theGraph = tmp_vol_graph.outerJoinVertices(const_denom_graph_v) { (vid, a, b) => if (vid < numj) a*b.get else a}.outerJoinVertices(fpGraph.vertices) { (vid, a, b) => if (vid < numj) a*b.get else b.get}

      //reailization of the operations in each loop to reflect runtime correctly (if not count(), runtime will not reflect correctly)
      theGraph.cache().vertices.count()
    }

    // save the output images to files
    val output = theGraph.vertices.filter{ case (vid, attr) => vid<numj}.filter{ case (vid, attr) => attr>0}
    val outfdir = "%s/output_%diter_%dnpart_test%s".format(rootdir,numIterations,numPartitions,Ntest)
    output.saveAsTextFile(outfdir)
  }
}



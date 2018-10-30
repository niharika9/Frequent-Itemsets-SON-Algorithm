import java.io.{BufferedWriter, FileWriter, PrintWriter}

import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.util.control.Breaks
import util.control.Breaks._


object Niharika_Gajam_SON_small{

  class KeyPartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = {
      val domain = key
      val code = (domain.hashCode % numPartitions)
      if (code < 0) {
        code + numPartitions  // Make it non-negative
      } else {
        code
      }
    }
    // Java equals method to let Spark compare our Partitioner objects
    override def equals(other: Any): Boolean = other match {
      case dnp: KeyPartitioner =>
        dnp.numPartitions == numPartitions
      case _ =>
        false
    }
  }


  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime

    val spark_config = new SparkConf().setAppName("Model Based Recommendation").setMaster("local[1]")
    val spark_context = new SparkContext(spark_config)

    val input_file = spark_context.textFile(args(0)).repartition(4).cache()

    val support_threshold = args(1).toInt

    val outputfile  = new BufferedWriter(new FileWriter(args(2)))
    val fileWriter = new PrintWriter(outputfile)


    var baskets_items : RDD[(String,Iterable[String])] = input_file.map(_.split(",")).map{ case Array(basket, item) => (basket,item)}.groupByKey()


    val chunk_threshold = (support_threshold.toInt)/(baskets_items.getNumPartitions)


    var set_size = 2


    /*  PHASE 1  MAP REDUCE  (A PRIORI ALGORITHM) */
    var output_phase1 : RDD[(List[String],Int)] = baskets_items.mapPartitionsWithIndex((index, elem) => {


      // each partition has its own element entries , baskets with items
      var basket_items_in_partition = ListBuffer.empty[(List[String])]
      elem.toList.map(x => basket_items_in_partition+=x._2.toList).iterator

      // now we have the baskets per partition

      // flatten all the item strings from all baskets to just one
      var step1  = basket_items_in_partition.flatten.groupBy(identity).map(x => (x._1, x._2.size))  /* TRY TO REDO THIS ONE */
      // size 1
      val cand_items = step1.filter(x => x._2 > chunk_threshold).map(x => (x._1,1))

      var freq_set = cand_items.map(x => {x._1}).toList
      var freq_item_size = freq_set.size



      var frequent_Items_output: ListBuffer[(List[String],Int)]= ListBuffer.empty

      var temp = cand_items.map(t=> (List(t._1), 1))

      temp.map( t=> {
        frequent_Items_output.append(t)
      })


      //println("freq items size in iteration : 1 : " + freq_item_size)

      /* looping starts here for A Priori ALGORITHM */
      while(freq_item_size != 0){

        val comb = freq_set.combinations(set_size).toList
        val count_each_itemset_size = comb.map(item_combination => {
          var count =0
          basket_items_in_partition.map(itemlist => {
            if(item_combination.forall(itemlist.contains)) count += 1
          })
          (item_combination.sorted,count)
        })


        var step2 = count_each_itemset_size.filter(x=> x._2 > chunk_threshold ).map(x=> (x._1,1))//.toMap
        var tp = step2.map(t=> (t._1,1))

        tp.map(t=> {
          frequent_Items_output.append(t)
        })
        var getlist_items_iteration = step2.map(x => x._1).toList
        //get the distinct items from this iteration
        freq_set = getlist_items_iteration.flatten.map(x=>x).toList.distinct
        freq_item_size = freq_set.size


        //println("freq items size in iteration : " + set_size + " : " + freq_item_size)
        set_size += 1
      }
      //println("looped till " + set_size + "combination of items")
      frequent_Items_output.toIterator

    }).partitionBy(new KeyPartitioner(numParts = 4)).reduceByKey{case(x,y) => 1}


    /*  PHASE 2  MAP REDUCE */
    val output_phase1_collect = output_phase1.collect()
    var output_phase2  = baskets_items.mapPartitionsWithIndex((index,elem)=>{

      var basket_items_in_partition = ListBuffer.empty[(List[String])]
      elem.toList.map(t => basket_items_in_partition += t._2.toList).toIterator

      /* PHASE2 Algorithm starts here */

      //
      val op2 = output_phase1_collect.map(local_freq_itemlist => {

        var count=0
        basket_items_in_partition.map(itemlist=>{

          if(local_freq_itemlist._1.forall(itemlist.contains)) count += 1
        })

        (local_freq_itemlist._1,count)
      })//.toIterator

      op2.toIterator

    }).partitionBy(new KeyPartitioner(numParts = 4)).reduceByKey{case(x,y)=>x+y}.filter(x=> { x._2 >= support_threshold}).map(elem => elem._1).sortBy(elem => elem.size)



    val loop = new Breaks;

    loop.breakable {

      var set_sz = 1
      while(true) {

        val op = output_phase2.filter(x => x.size == set_sz.toInt).map(x => x.toString).sortBy(x => x, true, numPartitions = 1).collect() //.collect.sorted.toString

        val sz = op.size
        var k = 1

        // println("size of sets " + op.size)

        if (op.size != 0) {
          for (i <- op) {
            var x = i.splitAt(4)
            var s = x._2

            if (k != sz)
              fileWriter.write(s + ", ")
            else
              fileWriter.write(s)

            k += 1
          }
          //println("writing to file")
          fileWriter.write("\n\n\n")

        } else {
          loop.break;
        }

        set_sz = set_sz + 1
      }
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println("Time: " + duration + " secs")

    fileWriter.close()
  }
}

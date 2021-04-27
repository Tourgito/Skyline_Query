import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import scala.math.{pow, sqrt }
import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream



//////////////////////////////////////////////////  The code take 3 inputs ////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////  #1 "Task1" if the user wants to execute the task 1 algorithm,  "Task3" if the user wants to execute the task 3 algorithm  ////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////  #2 The path of the dataset file ////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////  #3 The input k, solely the number of most k dominant points to return for task 3 ////////////////////////////////////////////////////////////////////////

object Algorithms{

 def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("WordCount")
  val sc = new SparkContext(conf)
  //val inputFile = "./Ergasia.txt"
  val inputArguments = args.toList
  val now = System.nanoTime
  val dataset = sc.textFile(inputArguments(1))   // load the dataset to a RDD
  //val dataset = sc.textFile(inputFile)
  //val dataset = sc.textFile("./" + a(0) + "/data/" + a(1) + "/" + a(2) + "/" + a(2) + ".txt")
  //val writer = new PrintWriter(new File("./" + a(0) + "/data/" + a(1) + "/" + a(2) + "/" + "task3Apotelesmata.txt"))
  val writer = new PrintWriter(new File("./Apotelesmata.txt"))  // The file to which the results are going to be write
  val executionTime = new PrintWriter(new File("./XronosEkteleshs.txt")) // The file to which the execution time is going to be write
  //val xronoi = new PrintWriter(new FileOutputStream(new File("./Xronoi.txt"), true))
 

  if (inputArguments(0) == "Task1")   // The code related to Task 1 is executed
   {
    val localSkylinePoints = dataset.map(x=>x.split(" ")).map(x => x.map( y => y.toDouble)).mapPartitions(skylineCalculation)  // Preprocess the data and calculate the local skyline points in each partition
    val globalSkylinePoints =  skylineCalculation(localSkylinePoints.collect.toIterator).toList.map(x => pointsTransformationFromListToString(x))  // Calculate the global skyline points
    globalSkylinePoints.foreach(x => writer.write(x + "\n")) // Write the global skyline points in a file
   }
  else if (inputArguments(0) == "Task3") // The code related to Task 3 is executed
   {
    val preprocessedData = dataset.map(x=>x.split(" ")).map(x => x.map( y => y.toDouble))  // Preprocess the data
    val localSkylinePoints = preprocessedData.mapPartitionsWithIndex((index, x) => skylineCalculationWithIndex(x,index))  // Calculate the local skyline points and their dominance score in each partition 
    val globalSkylinePoints =  skylineCalculation2(localSkylinePoints.collect.toIterator).toList // Calculate the global skyline points
    val broadcastedSkylinePoints = sc.broadcast(globalSkylinePoints) // Share the global skyline points as a broadcast variable to each worker
    val topKScoreSkylinePoints = preprocessedData.mapPartitionsWithIndex((index,x) => dominanceCalculation(x,index,broadcastedSkylinePoints)).reduceByKey(_+_).sortByKey().collect.toList // Calculate the dominance score of each skyline point for all the dataset
    val telikaPointsAndScores = (topKScoreSkylinePoints.map(x=>x._2) zip globalSkylinePoints.toList.map(x=> pointsTransformationFromListToString(x._1._1))).sortWith(_._1 > _._1).take(inputArguments(2).toInt) // Tranform the results to the appropriate form to write them in a file 
    telikaPointsAndScores.foreach(x => writer.write(x + "\n")) // Write the results to a file
    }
  else // The user gave as wrong input related to which task to execute 
    println("You must give word Task1 or Task3 as the first input")
  
  val timeElapsed = System.nanoTime - now
  //xronoi.append(a(0) + " " + a(1) + " " + a(2) + " " + timeElapsed.asInstanceOf[Double] / 1000000000.0 + "\n")
  executionTime.write(timeElapsed.asInstanceOf[Double] / 1000000000.0 + "\n")
  writer.close()
  executionTime.close()
  println("total time elapsed: "+ timeElapsed.asInstanceOf[Double] / 1000000000.0)



  sc.stop()
 }




//////////////////////////////////////////////////  Fuctions of Task 1 ////////////////////////////////////////////////////////////////////////


  def skylineCalculation(x: Iterator[Array[Double]]): Iterator[Array[Double]] =  // Calculates the skyline points of a set of points
  {
    var tempList = x.toList.map(x => calculateAxisOriginDistance(x)).sortBy(x => x._1).map(x => x._2)
    //var tempList = x.toList
    var i = 0
    var listLength = tempList.length
    while (i < listLength - 1) {
      var k = i + 1
        while (k < listLength) {
         if (xDominatesY(tempList(i),tempList(k))) {
           if (yNotDominatedByX(tempList(i),tempList(k)) != true)
            tempList = tempList.take(k) ++ tempList.drop(k + 1) //apokleiei to K pou egine shmeio pou egine dominated
            k = k - 1
            listLength = listLength - 1
          }
         else if (xDominatesY(tempList(k),tempList(i))) {
           if (yNotDominatedByX(tempList(k),tempList(i)) != true)
            tempList = tempList.take(i) ++ tempList.drop(i + 1)
            listLength = listLength - 1
            i = i - 1
            k = listLength
          }
        k = k + 1
        }
      i = i + 1
     }
    return tempList.toIterator
   }



  def calculateAxisOriginDistance(point: Array[Double]) : (Double, Array[Double]) =   // calculate the axis origin distance of a point.
  {  
    var r = 0.0
    point.foreach(x => r = r + pow(x,2))
    return (sqrt(r), point)

  }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



//////////////////////////////////////////////////  Fuctions of Task 3 ////////////////////////////////////////////////////////////////////////



  def dominanceCalculation(x: Iterator[Array[Double]], index:Int, skyline: Broadcast[List[((Array[Double], Int),Int)]] ): Iterator[(Int,Int)] =   //// Calculates which points the skyline points are dominating or not in a partition
  {
    val points = x.toList
    var comparisonsResults  = (skyline.value zip (List.range(0,skyline.value.length))).flatMap(skylinePoint => { 
                                                                                    var a  = List[(Int,Int)]()
                                                                                    if (skylinePoint._1._2 != index)
                                                                                      for ( y <- 0 to points.length - 1 ) {
                                                                                          if (xDominatesY(skylinePoint._1._1._1,points(y)))
                                                                                            a = (skylinePoint._2,1) :: a
                                                                                          else
                                                                                            a = (skylinePoint._2,0) :: a
                                                                                      }
                                                                                    else 
                                                                                        a = (skylinePoint._2, skylinePoint._1._1._2) :: a
                                                                                    a
                                                                                    }
                                                                                    
                                                                                    )
    return comparisonsResults.toIterator
  }


  def skylineCalculationWithIndex(x: Iterator[Array[Double]], index:Int): Iterator[((Array[Double], Int),Int)] = // Calculates the skyline points of a set of points and at the same time their dominance score in the dataset
  {
      var tempList = x.toList.map(x => calculateAxisOriginDistance(x)).sortBy(x => x._1).map(x => x._2)
      var i = 0
      var listLength = tempList.length
      var dropedPoints = List[Array[Double]]()
      var flag = true
      var dominationScores = List.fill(tempList.length)(0)  

      while (i <= listLength - 1) {
          flag = true
          var k = i + 1
          while (k < listLength) {
            if (xDominatesY(tempList(i),tempList(k))) {
              if (yNotDominatedByX(tempList(i),tempList(k)) != true)
              
                dropedPoints = tempList(k) :: dropedPoints
                tempList = tempList.take(k) ++ tempList.drop(k + 1) //apokleiei to K pou egine shmeio pou egine dominated
                dominationScores = dominationScores.take(k) ++ dominationScores.drop(k + 1) //apokleiei to K pou egine shmeio pou egine dominated
                k = k - 1
                listLength = listLength - 1
              }
            else if (xDominatesY(tempList(k),tempList(i))) {
              if (yNotDominatedByX(tempList(k),tempList(i)) != true)
                flag = false
                dropedPoints = tempList(i) :: dropedPoints
                tempList = tempList.take(i) ++ tempList.drop(i + 1)
                dominationScores = dominationScores.take(i) ++ dominationScores.drop(i + 1)
                listLength = listLength - 1
                i = i - 1
                k = listLength
              }
            k = k + 1
            }


         if (flag & dropedPoints.length != 0) {
              for (t <- 0 to dropedPoints.length - 1){
                     if (xDominatesY(tempList(i),dropedPoints(t))) 
                        if (yNotDominatedByX(tempList(i),dropedPoints(t)) != true)
                                dominationScores = dominationScores.updated(i, dominationScores(i) + 1)
                            }
            }

          i = i + 1
      }

      var partitionIndex = List.fill(tempList.length)(index)  
      return ((tempList zip dominationScores) zip partitionIndex).toIterator
      }



  def skylineCalculation2(x: Iterator[((Array[Double], Int),Int)]): Iterator[((Array[Double], Int),Int)] = // A variation of the Task 1 skylineFunction, which just takes a different type of input.
  {
    var tempList = x.toList
    //var tempList = x.toList
    var i = 0
    var listLength = tempList.length
    while (i < listLength - 1) {
      var k = i + 1
        while (k < listLength) {
         if (xDominatesY(tempList(i)._1._1,tempList(k)._1._1)) {
           if (yNotDominatedByX(tempList(i)._1._1,tempList(k)._1._1) != true)
            tempList = tempList.take(k) ++ tempList.drop(k + 1) //apokleiei to K pou egine shmeio pou egine dominated
            k = k - 1
            listLength = listLength - 1
          }
         else if (xDominatesY(tempList(k)._1._1,tempList(i)._1._1)) {
           if (yNotDominatedByX(tempList(k)._1._1,tempList(i)._1._1) != true)
            tempList = tempList.take(i) ++ tempList.drop(i + 1)
            listLength = listLength - 1
            i = i - 1
            k = listLength
          }
        k = k + 1
        }
      i = i + 1
     }
    return tempList.toIterator
   }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  

//////////////////////////////////////////////////  Fuctions Used In All The Tasks ////////////////////////////////////////////////////////////////////////



  def pointsTransformationFromListToString(point: (Array[Double])) : String =   // Transforms the data to the wright form i want as an outpout
    {
      var transformedPoint = ""
      for (i <- 0 to point.length - 1) {
        transformedPoint = transformedPoint + " " + point(i)
      }
      return transformedPoint
    }

  /*
   def pointsTransformationFromListToStringWithIndex(point: ((Array[Double], Int),Int)) :  ((String, Int),Int) = 
    {
      var transformedPoint = ""
      for (i <- 0 to point._1._1.length - 1) {
        transformedPoint = transformedPoint + " " + point._1._1(i)
      }
      return ((transformedPoint,point._1._2),point._2)
    } 
  */



  def xDominatesY(x: Array[Double], y:Array[Double]):Boolean = // Checks if a point dominates a other point
  {
    val size = x.length
    //var flag = false
    var flag = true
    var i = 0
    while (flag == true & i <= size -1)
    {
      if (x(i) >= y(i))
        flag = false
      i += 1
    }

    return flag
   }

  def yNotDominatedByX(x: Array[Double], y:Array[Double]):Boolean = // Checks if a point doesn't get dominated from a other point
  {
    val size = x.length
    var flag = false
    var i = 0
    while (flag == false & i <= size -1)
    {
      if (x(i) >= y(i))
        flag = true
      i += 1
    }

    return flag
  }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}





import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math._
import java.io._
import org.apache.spark.rdd._

object Dhanashree_Naik_task2 {
  def make_map(data:String): Map[(Int,Int),Double] = {
        
         val array = data.split(",")
         var dataMap = Map[(Int,Int),Double]()
         
         dataMap = Map((array(0).toInt,array(1).toInt)->(array(2).toDouble))
        return dataMap
        }
  def make_map2(data:String): Map[(Int, Int),Double] = {
        
         val array = data.split(",")
         var dataMap =Map[(Int, Int),Double]()
        
         dataMap = Map((array(0).toInt ,array(1).toInt)->1.toDouble)
         
        return dataMap
        }
  def createMap(userId:Int, MovieId:Int, ratings:Double) : Map[(Int,Int),Double]= {
    
    var dataMap =Map[(Int, Int),Double]()
    dataMap = Map((userId,MovieId)->(ratings))
    return dataMap
  }
  def createItemMap(MovieId:Int, userId:Int, ratings:Double) : Map[Int,(Int,Double)]= {
    
    var dataMap =Map[Int,(Int,Double)]()
    dataMap = Map(MovieId->(userId,ratings))
    return dataMap
  }
  def createAvg(MovieId:Int, userId_ratings: Iterable[(Int, Double)]) : Map[Int,Double]= {
    
    var dataMap =Map[Int,Double]()
   val num = userId_ratings.size
   var sum:Double = 0
   for (i <- userId_ratings ){
     sum += i._2 
   }
   dataMap = Map(MovieId->(sum/num))
    return dataMap
  }
  def findDiff(MovieId:Int, iter:(Iterable[(Int, Double)], Double)): Map[Int,scala.collection.mutable.Map[Int,Double]]= {
    var dataMap =scala.collection.immutable.Map[Int,scala.collection.mutable.Map[Int,Double]]()
    var temp = scala.collection.mutable.Map[Int,Double]()
    for (i<-iter._1 ){
       temp +=  i._1 -> (i._2 -iter._2)  
    }
    dataMap += (MovieId-> temp)
    return dataMap
  }
  
  def find_pcoff(MovieId1:Int, MovieId2:Int, normalMap:Map[Int,scala.collection.mutable.Map[Int,Double]]) : Double = {
    val i = normalMap(MovieId1)
    val j = normalMap(MovieId2)
    var sum:Double =0
    for (ii <- i.keys){      
      if (j.contains(ii)){
      sum = sum + j(ii) * i(ii)
      }
    }    
    var root1:Double =0
    var root2:Double =0    
    for (ii <- i.keys){
      root1 = root1 + i(ii)*i(ii)
    }
    for (ii <- j.keys){
      root2 = root2 + j(ii)*j(ii)
    }    
    val deno = (sqrt(root1)*sqrt(root2))
   
    if (deno!=0){
    val pcoff_initial = sum / deno
    //val pcoff = pcoff_initial
    val pcoff = pcoff_initial * Math.pow(Math.abs(pcoff_initial),(1.5))
    return pcoff}
    else return 0.000001
 
  } 
  
  
  def call_pcoff(it:Iterator[((Int, Int), Double)],normalMap:Map[Int,scala.collection.mutable.Map[Int,Double]],datamap: Map[(Int, Int),Double],userMap:Map[Int,Iterable[((Int, Int), Double)]], avg_rat:Map[Int,Double]) :  scala.collection.mutable.Map[(Int,Int),Double] ={
     val tempmap = scala.collection.mutable.Map[(Int,Int),Double]()
     
     for (i<- it)
     {
       var tempMap2 = scala.collection.mutable.Map[Int,Double]()
       var pcoff_datamap =  Map[(Int,Int),Double]()
       val MovieId = i._1 ._2 
       val user = i._1 ._1
       if (normalMap.contains(MovieId)){
       var flag = 1
       for (j <- normalMap.keys)
       {
          if (j != MovieId)
          {
            if (normalMap(j).contains(user))
            {
            flag = 0
            pcoff_datamap += (MovieId,j)-> find_pcoff(MovieId,j,normalMap)

            }
            }
            }
       if (flag ==1){
         tempmap += (user,MovieId) ->  avg_rat(MovieId)
       }
       else{
       val nears = pcoff_datamap.toSeq.sortBy(_._2).reverse.take(20)
        var numerator:Double =0
        var denom:Double = 0
        var denom2:Double = 0
        for ( n<-nears){
          
          var u = datamap(user,n._1 ._2)
          numerator = numerator + (u*n._2 )
          denom = denom + Math.abs(n._2)
          
        }
       if ((numerator/denom)>5){
          
          tempmap += (user,MovieId)-> (avg_rat(MovieId))
          
        }
       else if ((numerator/denom)<0){
          
          tempmap += (user,MovieId)-> (avg_rat(MovieId))
          
        }
        else{
        tempmap += (user,MovieId)->(numerator/denom)}
        
       }}
              
       else{
        if (userMap.contains(user)){
        val vect = userMap(user)
	    var sum:Double=0
	    val c:Double = vect.size
	    for (i <- vect){
	    	sum=sum + i._2 }  
        
	    tempmap += (user,MovieId) -> sum/c
        }
        else{
        tempmap += (user,MovieId) -> 2.5 
        }
       }}
       
        
    
   return tempmap 
  }
  
  def main(args: Array[String]) {
 
 val ratingFile = args(0)
  val testFile = args(1)
  val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val output = new PrintWriter(new File("Dhanashree_Naik_result_task2.txt" ))
  
  var ratingLines = sc.textFile(ratingFile,4).cache()
  var testLines = sc.textFile(testFile,4).cache()
  var rate_head =  ratingLines.first
  var test_head = testLines.first  
  ratingLines =ratingLines.filter(f=>f != rate_head)
  testLines =testLines.filter(f=>f != test_head)
  
  val data_rate = ratingLines.flatMap(f=> make_map(f))
  
  val data_test = testLines.flatMap(f=>make_map2(f))
  var testing_data = data_rate.join(data_test)

  
  var test_map = testing_data.flatMap(f=>createMap(f._1._1 ,f._1 ._2  ,f._2 ._1 ))
  val new_map = data_rate.subtractByKey(test_map)
  
  val itemMap = new_map.flatMap(f=>createItemMap(f._1 ._2 ,f._1 ._1 ,f._2 ))
  val group_item = itemMap.groupByKey()
  
  
  val avg_item_rating = group_item.flatMap(f=>createAvg(f._1 ,f._2 ))
 
  
  val join_avg = group_item.join(avg_item_rating)
  
  val normalized_ratings = join_avg.flatMap(f=>findDiff(f._1 ,f._2 ))

  var normal_rat = normalized_ratings.collect.toMap
  var item_rat = data_rate.collect.toMap
  var avg_rat = avg_item_rating.collect.toMap
  val userMap=new_map.groupBy(f=>f._1 ._1 ).collect.toMap
  

  val finaldata = test_map.mapPartitions(f=> call_pcoff(f,normal_rat,item_rat,userMap,avg_rat).iterator)

  
  val rms_map = test_map.join(finaldata)
  
  val MSE = rms_map.map { case ((user, product), (r1, r2)) =>
	  val err = Math.abs((r1 - r2))
	  err * err
	}.mean()

   var v1 = new scala.collection.mutable.ListBuffer[Int]()
    var v2 = new scala.collection.mutable.ListBuffer[Int]()
    var v3= new scala.collection.mutable.ListBuffer[Int]()
    var v4 = new scala.collection.mutable.ListBuffer[Int]()
    var v5 = new scala.collection.mutable.ListBuffer[Int]()
    
    
    rms_map.collect.foreach(f=>
    if (abs(f._2 ._1 -f._2 ._2 )>=0 && abs(f._2 ._1 -f._2 ._2 )<1){ v1 +=1 }
    else if (abs(f._2 ._1 -f._2 ._2 )>=1 && abs(f._2 ._1 -f._2 ._2 )<2){v2+=1}
    else if (abs(f._2 ._1 -f._2 ._2 )>=2 && abs(f._2 ._1 -f._2 ._2 )<3){v3+=1}
    else if (abs(f._2 ._1 -f._2 ._2 )>=3 && abs(f._2 ._1 -f._2 ._2 )<4){v4+=1}
    else if (abs(f._2 ._1 -f._2 ._2 )>=4){v5+=1}
    )    

     println(">=0 and <1: "+v1.length)
    println(">=1 and <2: "+v2.length)
    println(">=2 and <3: "+v3.length)
    println(">=3 and <4: "+v4.length)
    println(">=4: "+v5.length)
    println("RMSE: "+Math.sqrt(MSE))
   
	
    var flag = 0
	output.write("UserId,MovieId,Pred_rating"+"\n")
	finaldata.toArray.sortBy(f=>(f._1 ._1,f._1 ._2 ) ).foreach({f=> 
	if  (flag == 0){
	output.write(f._1._1 + "," + f._1 ._2+ ","+ f._2)
	flag=1}
	else{output.write("\n"+f._1._1 + "," + f._1 ._2+ ","+ f._2)};
	}
	)
    output.close()
  
    
  }
  

}


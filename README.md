# big_data_predict
Based on Naive Beyes algorithm.



## 预测飞机是否晚点
# 步骤一 数据预处理
在数据集所给出的数据中，通过经验与和助教一起反复尝试，我们发现飞机是否晚点起飞与“日期”和“起飞时间”这两个因素有较大关联。因此，对数据进行预处理如下：
![image](https://user-images.githubusercontent.com/44899736/147671658-a5d4949a-0fd6-4e89-bba1-315eb9f101ae.png)
- 第一列为飞机起飞的日期为周几，第二列为起飞的时间，第三列为起飞延迟的时间。若起飞延迟时间为0，则为未延误，若起飞延迟时间为正数，则为延误。
- 通过代码，对第三列起飞延误时间进行布尔转换打标签。
"
  if(theClassification.toInt>0){
    label="1"
  }
  else{
    label="0"
  }
"

# 步骤二 上传文件
hadoop fs -put train.csv .
hadoop fs -put test.csv .


# 步骤三 运行程序
"""
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.{ArrayBuffer, HashMap}


val file = sc.textFile("train.csv", 2)
val pair = file.flatMap(line=>{
  val tokens:Array[String]=line.split(",")
  val classificationIndex=tokens.length-1
  val theClassification:String=tokens(classificationIndex)
  var label=""
  if(theClassification.toInt>0){
    label="1"
  }
  else{
    label="0"
  }
  
  
  var result=new Array[(String,String)](classificationIndex+1)
  var i=0
  var time=0
  while(i < classificationIndex){
    if(i==1){
      time=tokens(i).toInt / 100
      result(i)=((time.toString,label))
      
    }
    else{
      result(i)=((tokens(i),label))
    }
    
    i+=1
  }
  result(i)=("CLASS",label)
  result
}
).map(pair => (pair, 1))
val counts=pair.reduceByKey(_+_)
val countsAsMap=counts.collectAsMap()
var CLASSIFICATION=new ArrayBuffer[String]()
var PT=new HashMap[(String,String),Double]
for((key,value)<-countsAsMap){
  val classification=key._2
  if(key._1=="CLASS"){
    CLASSIFICATION+=classification
    PT(key)=value
  }
  else{
    val sum = countsAsMap(("CLASS",key._2))
    if(value==null){
      PT(key)=0.0
    }
    else{
      PT(key)=value.toDouble/sum.toDouble
    }
  }
}
var trainingSize=0.0
for(classification<-CLASSIFICATION){
  trainingSize+=PT(("CLASS",classification))
}
for(classification<-CLASSIFICATION){
  PT(("CLASS",classification))/=trainingSize
}
val PT2save=PT.toArray
val ptRDD = sc.parallelize(PT2save, 2)
//ptRDD.saveAsTextFile("PT")
val CLFRDD=sc.parallelize(CLASSIFICATION, 1)
//CLFRDD.saveAsTextFile("CLASSIFICATION")


val testdata = sc.textFile("test.csv", 1)
val broadcastPT=sc.broadcast(PT)
val broadcastCLASS=sc.broadcast(CLASSIFICATION)
val classified=testdata.map(line=>{
  val attributes:Array[String]=line.split(",")
  val PT=broadcastPT.value
  val CLASS=broadcastCLASS.value
  var selectedCLASS:String=null
  var maxProbility:Double=0
  for(aCLASS<-CLASS){
    var postprob:Double=PT(("CLASS",aCLASS))
    for(i<-0 until attributes.length){
      var prob:Double=0.0
      if(PT.contains((attributes(i),aCLASS))){
        prob=PT((attributes(i),aCLASS))
        println("P("+attributes(i)+"|"+aCLASS+") "+prob)
      }
      postprob*=prob
    }
    if(selectedCLASS==null){
      selectedCLASS=aCLASS
      maxProbility=postprob
    }
    if(postprob>maxProbility){
      selectedCLASS=aCLASS
      maxProbility=postprob
    }
  }
  (line,selectedCLASS)
})
classified.map(x=>x._1+" "+x._2).collect

"""
### 结果输出
![8b2fa81566785d7ca1f79c537a42235](https://user-images.githubusercontent.com/44899736/147672633-3001f94e-8f3f-459d-89a1-028dd9f18218.png)
0为未延误，1为延误。

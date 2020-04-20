package findDS

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}

object FindDS {


  val pathSet = "./resources/sets"

  val spark = SparkSession.builder.appName("SparkSQL")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()



  def main(args: Array[String]): Unit = {


    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val dsInfo = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").
      load("./resources/datasetsInformation.csv")


    var mapDS = Map[String, DataFrame]()
    dsInfo.select("filename").collect().foreach{case Row( filename: String) => mapDS = mapDS + (filename -> readDS(filename))}



    val listFiles = mapDS.keySet.toSeq
    val size = listFiles.size

    var linesNom: Seq[String] = Nil
    var linesNum: Seq[String] = Nil

    for(i <- 0 to size-2){
      for(j <- i+1 to size-1){
//        println(s"${listFiles(i)} __ ${listFiles(j)}")
        val ds1 = mapDS.get(listFiles(i)).get
        val ds2 =mapDS.get(listFiles(j)).get
//
//        ds1.printSchema()
//        ds2.printSchema()

        linesNom = linesNom ++ findCandidates(listFiles(i),ds1,listFiles(j),ds2,"nominal")
        linesNum = linesNum ++ findCandidates(listFiles(i),ds1,listFiles(j),ds2,"numeric")
      }
    }
    linesNom = "file1,att1,sizeDistinct1,file2,att2,sizeDistinct2,joinSize,containment1,containment2,jaccard\n" +: linesNom
    writeFile("./resources/candidatesNom.csv",linesNom)

    linesNum = "file1,att1,sizeDistinct1,file2,att2,sizeDistinct2,joinSize,containment1,containment2,jaccard\n" +: linesNum
    writeFile("./resources/candidatesNum.csv",linesNum)

    spark.stop()

  }

  def toSets(ds: DataFrame, filename:String ,att: String) = {
    val name = s"${filename}.${att}"
    ds.select(att).distinct().write.parquet(s"${pathSet}/${name}")
  }

  def readDS(filename: String):DataFrame = {
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "true").
      load(s"./resources/raw/$filename")
  }

  def findCandidates(f1: String, ds1:DataFrame, f2: String, ds2: DataFrame, typeAtt: String): Seq[String]= typeAtt match{
      case "nominal" =>
        val names1 = ds1.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
        val names2 = ds2.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)
        find(f1,ds1,f2,ds2,names1,names2)

      case "numeric" =>
        val names1 = ds1.schema.filter(a => a.dataType.isInstanceOf[IntegerType]).map(a => a.name)
        val names2 = ds2.schema.filter(a => a.dataType.isInstanceOf[IntegerType]).map(a => a.name)
        find(f1,ds1,f2,ds2,names1,names2)
  }

  def find(f1: String, ds1:DataFrame, f2: String, ds2: DataFrame, names1:Seq[String],names2:Seq[String]): Seq[String]={
    var comparisons: Seq[String] = Nil
    for( att1 <- names1 ){
      for( att2 <- names2 ){

        var d1 = ds1
        var d2 = ds2

        try {
          d1 = spark.read.parquet(s"${pathSet}/${f1}.${att1}")
        } catch {
          case e:
            AnalysisException => println("Couldn't find that file.")
            d1 = ds1.select(att1).distinct()
            toSets(ds1, f1, att1)
        }
        try {
          d2 = spark.read.parquet(s"${pathSet}/${f2}.${att2}")
        } catch {
          case e:
            AnalysisException => println("Couldn't find that file.")
            d2 = ds2.select(att2).distinct()
            toSets(ds2, f2, att2)
        }

        val size1 = d1.count().toDouble
        val size2 = d2.count().toDouble

        val joinExpression = d1.col(att1) === d2.col(att2)
        val j = d1.join(d2,joinExpression)
        val tuplas = j.count().toDouble
        val containment1 = tuplas.toDouble/size1
        val containment2 = tuplas.toDouble/size2
        val jaccard = tuplas.toDouble/(size1+size2)
        val tmp = s"${f1},${att1},${size1},${f2},${att2},${size2},${tuplas},${containment1},${containment2},${jaccard}\n"
        println(tmp)
        comparisons = comparisons :+ tmp
      }
    }
    comparisons
  }

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }

}

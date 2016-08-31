package com.uga.datascience.naivebayes;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import com.uga.datascience.naivebayes.beans.LabelProbability;
import com.uga.datascience.naivebayes.beans.VocabProbability;
import com.uga.datascience.naivebayes.beans.WordCount;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");
        System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
     	SparkConf conf = new SparkConf().setAppName("App").setMaster("local").set("spark.sql.warehouse.dir", "/spark-warehouse");
		SparkContext ctx=new SparkContext(conf);
		SparkSession sparkSession=SparkSession.builder().appName("Naive Bayes").config(conf).getOrCreate();
		SQLContext sqlCtx=sparkSession.sqlContext();
		LongAccumulator documentCount=ctx.longAccumulator();
		LongAccumulator vocabuloryCount=ctx.longAccumulator();
		JavaRDD<String> textfile = sparkSession.read().textFile(args[0]).javaRDD();
		Properties prop = Utilities.loadStopWords("stopwords.properties");
		String stop = prop.getProperty("stopwords");	
		Pattern stopwordsPattern = Pattern.compile("\\b(" + stop + ")\\b\\s?", Pattern.CASE_INSENSITIVE);
		JavaRDD<String> rddY = textfile.map(new Function<String, String>() {
			private static final long serialVersionUID = -7726978724451251690L;

			@Override
			public String call(String string) throws Exception {
			String temp = string.toLowerCase();
			Matcher m = stopwordsPattern.matcher(temp);
			String processedText = m.replaceAll("");
			String removeSpecial=processedText.replaceAll("&(quot;)","").replaceAll("[:;,.+/{}()$*%]", "").replaceAll("\\b\\d{1,3}\\b\\s?","").replaceAll("\\b\\w{1,2}\\b","");
			return removeSpecial;
			}

		});
		Utilities ut=new Utilities();
		switch (args[3]) {
		case "LEARNING":
			JavaPairRDD<Integer, String> linePair=ut.makeLinePair(rddY);
			long docCount=linePair.count();
			documentCount.add(linePair.count());
			JavaRDD<String> uniqueVocab=Utilities.buildVocabulary(rddY);
			vocabuloryCount.add(uniqueVocab.count());
		    JavaRDD<String> labelFile = sparkSession.read().textFile(args[1]).javaRDD();
		    JavaRDD<String[]> filteredLabelFile=Utilities.getlable(labelFile);
		    JavaPairRDD<Integer, String[]> pairedLabel=ut.makeLabelPair(filteredLabelFile);
		    JavaPairRDD<Integer, Tuple2<String, String[]>> docClasificationPair=linePair.join(pairedLabel);
		    JavaPairRDD<Integer, Tuple2<String, String[]>> sortedDocClasificationPair=docClasificationPair.sortByKey();
		    List<Tuple2<Integer, Tuple2<String, String[]>>> a=sortedDocClasificationPair.collect();
			JavaRDD<List<WordCount>> wordCountRDD=uniqueVocab.map(new Function<String, List<WordCount>>() {
				private static final long serialVersionUID = 7201909674505781255L;

				@Override
				public List<WordCount> call(String line) throws Exception {
					
					List<WordCount> wordCountList=ut.getWordCountList(line, a);
					return wordCountList;
				}
			});
			JavaRDD<WordCount> temp= wordCountRDD.flatMap(new FlatMapFunction<List<WordCount>, WordCount>() {

					private static final long serialVersionUID = -8395782926696124478L;

					@Override
					public Iterator<WordCount> call(List<WordCount> input) throws Exception {
						return input.iterator();
					}
				});

			Dataset<Row> wordCountDF=sparkSession.createDataFrame(temp, WordCount.class);
			wordCountDF.createOrReplaceTempView("wordCountView");

			
			Dataset<Row> ccatWordsDF=ccatWords(sqlCtx, sparkSession);
			Dataset<Row> ecatWordsDF=ecatWords(sqlCtx, sparkSession);
			Dataset<Row> gcatWordsDF=gcatWords(sqlCtx, sparkSession);
			Dataset<Row> mcatWordsDF=mcatWords(sqlCtx, sparkSession);

			ccatWordsDF.createOrReplaceTempView("ccatWordDF");
			ecatWordsDF.createOrReplaceTempView("ecatWordDF");
			gcatWordsDF.createOrReplaceTempView("gcatWordDF");
			mcatWordsDF.createOrReplaceTempView("mcatWordDF");

			List<Row> ccatWordsDFList=ccatWordsDF.collectAsList();
			List<Row> ecatWordsDFList=ecatWordsDF.collectAsList();
			List<Row> mcatWordsDFList=mcatWordsDF.collectAsList();
			List<Row> gcatWordsDFList=gcatWordsDF.collectAsList();
			long  vocabCount=uniqueVocab.count();
			long ccatWordsDFCount=ccatWordsDF.count();
			long ecatWordsDFCount=ecatWordsDF.count();
			long mcatWordsDFCount=mcatWordsDF.count();
			long gcatWordsDFCount=gcatWordsDF.count();


			JavaRDD<VocabProbability> vocabProbabilityRDD=uniqueVocab.map(new Function<String, VocabProbability>() {
					private static final long serialVersionUID = 1L;

						@Override
						public VocabProbability call(String uniqueWord) throws Exception {
							VocabProbability vocabProbability=ut.getVocabProbability(uniqueWord, ccatWordsDFList, ecatWordsDFList, 
									mcatWordsDFList, gcatWordsDFList, vocabCount, ccatWordsDFCount, 
									ecatWordsDFCount, mcatWordsDFCount, gcatWordsDFCount);
									return vocabProbability;
						}
				});
			vocabProbabilityRDD.saveAsTextFile(args[2]);
			long calculateCCATCount=calculateCCATCount(sqlCtx, sparkSession);
			long ealculateCCATCount=calculateECATCount(sqlCtx, sparkSession);
			long malculateCCATCount=calculateMCATCount(sqlCtx, sparkSession);
			long galculateCCATCount=calculateGCATCount(sqlCtx, sparkSession);
			LabelProbability labelProb=ut.calculateLabelProbability(docCount, calculateCCATCount, ealculateCCATCount,
					malculateCCATCount, galculateCCATCount);
			Encoder<LabelProbability> labelProbEncoder = Encoders.bean(LabelProbability.class);
			Dataset<LabelProbability> labelProbDS = sparkSession.createDataset(
					  Collections.singletonList(labelProb),
					  labelProbEncoder
					);
			labelProbDS.javaRDD().saveAsTextFile("C:\\Users\\dhara\\Downloads\\DataScience\\labelProb.txt");
			
	
			break;
  
		case "TESTING":
			
			
			break;
		default:
			System.out.println("Please specify correct mode: 'LEARNING' or 'TRAINING'");
			break;
		}
			}
	
	
	public static Dataset<Row> ccatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> ccatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) AS frequencyCCAT,a1.uniqueword FROM wordCountView a1 "
				+ " WHERE (a1.label_1='CCAT' OR a1.label_2='CCAT' OR a1.label_3='CCAT' OR a1.label_4='CCAT') GROUP BY a1.uniqueword");
	
	     return ccatWordDF;
	}
	
	public static Dataset<Row> ecatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> ecatWordDF=sqlCtx.sql("SELECT COUNT(a1.frequency) as frequencyECAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='ECAT' OR a1.label_2='ECAT' OR a1.label_3='ECAT' OR a1.label_4='ECAT') GROUP BY a1.uniqueword");
	
	   
	     return ecatWordDF;
	}
	
	public static Dataset<Row> gcatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> gcatWordDF=sqlCtx.sql("SELECT COUNT(a1.frequency) as frequencyGCAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='GCAT' OR a1.label_2='GCAT' OR a1.label_3='GCAT' OR a1.label_4='GCAT') GROUP BY a1.uniqueword");
	
	  
	     return gcatWordDF;
	}
	public static Dataset<Row> mcatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> mcatWordDF=sqlCtx.sql("SELECT COUNT(a1.frequency) as frequencyMCAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='MCAT' OR a1.label_2='MCAT' OR a1.label_3='MCAT' OR a1.label_4='MCAT') GROUP BY a1.uniqueword");
	
	    
	     return mcatWordDF;
	}
	
   
	 public static long calculateCCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT DISTINCT(docId) FROM wordCountView where label_1='CCAT' or label_2='CCAT' or label_3='CCAT' or label_4='CCAT' ");
	     return docId.count();
	 }
	 
	 public static long calculateECATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT DISTINCT(docId) FROM wordCountView where label_1='ECAT' or label_2='ECAT' or label_3='ECAT' or label_4='ECAT' ");
	     return docId.count();
	 }
	 
	 public static long calculateMCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT DISTINCT(docId) FROM wordCountView where label_1='MCAT' or label_2='MCAT' or label_3='MCAT' or label_4='MCAT' ");
	     return docId.count();
	 }
	 public static long calculateGCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT DISTINCT(docId) FROM wordCountView where label_1='GCAT' or label_2='GCAT' or label_3='GCAT' or label_4='GCAT' ");
	     return docId.count();
	 }
	 
	
}

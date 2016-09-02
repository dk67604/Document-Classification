package com.uga.datascience.naivebayes;

import java.util.ArrayList;
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

import com.uga.datascience.naivebayes.beans.DocClassifer;
import com.uga.datascience.naivebayes.beans.DocClassifierPair;
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
			String temp = string.toLowerCase().replaceAll(" +", " ");
			String removeSpecial=temp.replaceAll("&(quot;)","").replaceAll("&(amps)", "").replaceAll("[:;,.+/{}()&$*%'?]", "").replaceAll("\\b\\d{1,9}\\b\\s?","").replaceAll("\\b\\w{1,3}\\b","").trim();
			Matcher m = stopwordsPattern.matcher(removeSpecial);
			String processedText = m.replaceAll(" ");
			
			String removeHyphen=processedText.replaceAll("-{1,2}", " ").replaceAll(" +", " ").trim();
			
		//	System.out.println(removeHyphen);
			return removeHyphen;
			}

		});
		//rddY.saveAsTextFile("C:\\Users\\dhara\\Downloads\\DataScience\\preprocess.txt");
		Utilities ut=new Utilities();
		JavaPairRDD<Integer, String> linePair=ut.makeLinePair(rddY);
		long docCount=linePair.count();
		documentCount.add(linePair.count());
		
		switch (args[4]) {
		case "LEARNING":
			
			JavaRDD<String> uniqueVocab=Utilities.buildVocabulary(rddY);
			vocabuloryCount.add(uniqueVocab.count());
		    JavaRDD<String> labelFile = sparkSession.read().textFile(args[1]).javaRDD();
		    JavaRDD<String[]> filteredLabelFile=Utilities.getlable(labelFile);
		    JavaPairRDD<Integer, String[]> pairedLabel=ut.makeLabelPair(filteredLabelFile);
		    JavaPairRDD<Integer, Tuple2<String, String[]>> docClasificationPair=linePair.join(pairedLabel);
		    JavaPairRDD<Integer, Tuple2<String, String[]>> sortedDocClasificationPair=docClasificationPair.sortByKey();
		   
		    JavaRDD<DocClassifierPair> docClasfierPairRDD=sortedDocClasificationPair.map(new Function<Tuple2<Integer,Tuple2<String,String[]>>, DocClassifierPair>() {
				private static final long serialVersionUID = 6694664017226133614L;
				@Override
				public DocClassifierPair call(Tuple2<Integer, Tuple2<String, String[]>> input) throws Exception {
							  
					DocClassifierPair docClasf=new DocClassifierPair();
						   docClasf.setDocId(String.valueOf(input._1()));
							if(input._2()._2.length==1){
								docClasf.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
							}
							if(input._2()._2.length==2){
								docClasf.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
								docClasf.setLabel_2(input._2()._2[1]!=null?input._2()._2[1].trim():null);
							}
							if(input._2()._2.length==3){
								docClasf.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
								docClasf.setLabel_2(input._2()._2[1]!=null?input._2()._2[1].trim():null);
								docClasf.setLabel_3(input._2()._2[2]!=null?input._2()._2[2].trim():null);
							}
							if(input._2()._2.length==4){
								docClasf.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
								docClasf.setLabel_2(input._2()._2[1]!=null?input._2()._2[1].trim():null);
								docClasf.setLabel_3(input._2()._2[2]!=null?input._2()._2[2].trim():null);
								docClasf.setLabel_4(input._2()._2[3]!=null?input._2()._2[3].trim():null);
							}
					   
					   return docClasf;
				}
			});
		    
			Dataset<Row> docClasfierPairDF=sparkSession.createDataFrame(docClasfierPairRDD, DocClassifierPair.class);
			List<Tuple2<Integer, Tuple2<String, String[]>>> sortedCollectionList=sortedDocClasificationPair.collect();
			docClasfierPairDF.createOrReplaceTempView("docClasfierPairDF");
		    JavaRDD<List<WordCount>> wordCountRDD=uniqueVocab.map(new Function<String, List<WordCount>>() {
				private static final long serialVersionUID = 7201909674505781255L;

				@Override
				public List<WordCount> call(String line) throws Exception {
					
					List<WordCount> wordCountList=ut.getWordCountList(line, sortedCollectionList);
					return wordCountList;
				}
			});
			//wordCountRDD.saveAsTextFile("C:\\Users\\dhara\\Downloads\\DataScience\\wordCount.txt");
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
			ccatWordsDF.createOrReplaceTempView("ccatWordsDF");
			ecatWordsDF.createOrReplaceTempView("ecatWordsDF");
			gcatWordsDF.createOrReplaceTempView("gcatWordsDF");
			mcatWordsDF.createOrReplaceTempView("mcatWordsDF");
			List<Row> ccatWordsDFList=ccatWordsDF.collectAsList();
			List<Row> ecatWordsDFList=ecatWordsDF.collectAsList();
			List<Row> mcatWordsDFList=mcatWordsDF.collectAsList();
			List<Row> gcatWordsDFList=gcatWordsDF.collectAsList();
			Dataset<Row> sumFrequencyCCAT=getCCATSum(sqlCtx, sparkSession);
			Dataset<Row> sumFrequencyECAT=getECATSum(sqlCtx, sparkSession);
			Dataset<Row> sumFrequencyMCAT=getMCATSum(sqlCtx, sparkSession);
			Dataset<Row> sumFrequencyGCAT=getGCATSum(sqlCtx, sparkSession);
			List<Row> ccatFrequency=  sumFrequencyCCAT.collectAsList();
			List<Row> ecatFrequency=  sumFrequencyECAT.collectAsList();
			List<Row> mcatFrequency=  sumFrequencyMCAT.collectAsList();
			List<Row> gcatFrequency=  sumFrequencyGCAT.collectAsList();
			long  vocabCount=uniqueVocab.count();
		//	System.out.println("vocabCount"+vocabCount);
			long ccatWordsDFCount=ccatFrequency.get(0).get(0)!=null?ccatFrequency.get(0).getLong(0):0;
			long ecatWordsDFCount=ecatFrequency.get(0).get(0)!=null?ecatFrequency.get(0).getLong(0):0;
			long mcatWordsDFCount=mcatFrequency.get(0).get(0)!=null?mcatFrequency.get(0).getLong(0):0;
			long gcatWordsDFCount=gcatFrequency.get(0).get(0)!=null?gcatFrequency.get(0).getLong(0):0;
           
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
			JavaRDD<String> parsedVocabProb=vocabProbabilityRDD.map(new Function<VocabProbability, String>() {

				private static final long serialVersionUID = 9091473001297751596L;

				@Override
				public String call(VocabProbability vocabProb ) throws Exception {
					// TODO Auto-generated method stub
					return vocabProb.getUniqueWord()+","+vocabProb.getProbabilityCCAT()+","+vocabProb.getProbabilityECAT()+","+vocabProb.getProbabilityMCAT()+","+vocabProb.getProbabilityGCAT();
				}
			});
			parsedVocabProb.saveAsTextFile(args[2]);
			
			long calculateCCATCount=calculateCCATCount(sqlCtx, sparkSession);
			long calculateECATCount=calculateECATCount(sqlCtx, sparkSession);
			long calculateMCATCount=calculateMCATCount(sqlCtx, sparkSession);
			long calculateGCATCount=calculateGCATCount(sqlCtx, sparkSession);
			    System.out.println("ccatWordsDFCount"+calculateCCATCount);
	            System.out.println("ecatWordsDFCount"+calculateECATCount);
	            System.out.println("mcatWordsDFCount"+calculateMCATCount);
	            System.out.println("gcatWordsDFCount"+calculateGCATCount);
			LabelProbability labelProb=ut.calculateLabelProbability(docCount, calculateCCATCount, calculateECATCount,
					calculateMCATCount, calculateGCATCount);
			Encoder<LabelProbability> labelProbEncoder = Encoders.bean(LabelProbability.class);
			Dataset<LabelProbability> labelProbDS = sparkSession.createDataset(
					  Collections.singletonList(labelProb),
					  labelProbEncoder
					);
			JavaRDD<String> parsedLabelProbs=labelProbDS.javaRDD().map(new Function<LabelProbability, String>() {
				private static final long serialVersionUID = 8300469312728020207L;

				@Override
				public String call(LabelProbability labelProbibility) throws Exception {
					return labelProbibility.getCcatLabelProb()+","+labelProbibility.getEcatLabelProb()
					+","+labelProbibility.getMcatLabelProb()+","+labelProbibility.getGcatLabelProb();
				}
			
			});
			parsedLabelProbs.saveAsTextFile(args[3]);
				break;
  
	case "TESTING":
			final long THERSHOLD=400000;
			boolean flag=false;
			
			JavaRDD<VocabProbability> vocabProb=sparkSession.read().textFile(args[1]).
			javaRDD().map(new Function<String, VocabProbability>() {
				private static final long serialVersionUID = 8093034116214389868L;
				long uniqueWordCount=0;
				@Override
				public VocabProbability call(String input) throws Exception {
					String[] inputLine=input.split(",");
					VocabProbability vocabProb=new VocabProbability();
					vocabProb.setUniqueWord(inputLine[0]);
					vocabProb.setProbabilityCCAT(Double.valueOf(inputLine[1]));
					vocabProb.setProbabilityECAT(Double.valueOf(inputLine[2]));
					vocabProb.setProbabilityMCAT(Double.valueOf(inputLine[3]));
					vocabProb.setProbabilityGCAT(Double.valueOf(inputLine[4]));
					vocabProb.setUniqueWordId(++uniqueWordCount);
					return vocabProb;
				}
			});
		//	List<VocabProbability> uniquevocabProbability=vocabProb.collect();
			//System.out.println(uniquevocabProbability.get(10).toString());
			JavaRDD<LabelProbability> labelProbabilities=sparkSession.read().textFile(args[2]).
					javaRDD().map(new Function<String, LabelProbability>() {

						private static final long serialVersionUID = 5397031477307806421L;

						@Override
						public LabelProbability call(String labelsProb) throws Exception {
							LabelProbability labelProbObj=new LabelProbability();
							String[] tempArray=labelsProb.split(",");
							labelProbObj.setCcatLabelProb(Double.valueOf(tempArray[0]));
							labelProbObj.setEcatLabelProb(Double.valueOf(tempArray[1]));
							labelProbObj.setMcatLabelProb(Double.valueOf(tempArray[2]));
							labelProbObj.setGcatLabelProb(Double.valueOf(tempArray[3]));
							return labelProbObj;
						}
					});
			List<LabelProbability> listLabelProb=labelProbabilities.collect();
			Dataset<Row> vocabProbDF=sparkSession.createDataFrame(vocabProb, VocabProbability.class);
			
			vocabProbDF.createOrReplaceTempView("testingVocabProbDFView");
			long vocabProbDFCount =vocabProbDF.count();
			if(vocabProbDFCount<THERSHOLD){
				flag=true;
			}
			if(flag){
				
				Dataset<Row> partition=createPartion(sqlCtx, sparkSession, 1, vocabProbDFCount);
				partition.show();
				List<Row> uniqueVocabPartition=partition.collectAsList();
				JavaRDD<DocClassifer> docClassfierRDD=linePair.map(new Function<Tuple2<Integer,String>, DocClassifer>() {

					private static final long serialVersionUID = -4645083522263678220L;

					@Override
					public DocClassifer call(Tuple2<Integer, String> document) throws Exception {
					  DocClassifer docClassifier=new DocClassifer();
					  docClassifier=ut.getDocClassification(document,uniqueVocabPartition, listLabelProb);
						return docClassifier;
					}
				});
			//	System.out.println("jj:"+docClassfierRDD.count());
				docClassfierRDD.saveAsTextFile(args[3]);
				flag=true;
			}
			if(!flag){
				
				Dataset<Row> partition1=createPartion(sqlCtx, sparkSession, 1, THERSHOLD);
				Dataset<Row> partition2=createPartion(sqlCtx, sparkSession, THERSHOLD+1, vocabProbDFCount);
				List<Row> uniqueVocabPartition1=partition1.collectAsList();
				List<Row> uniqueVocabPartition2=partition2.collectAsList();
				JavaRDD<DocClassifer> docClassfierRDD1=linePair.map(new Function<Tuple2<Integer,String>, DocClassifer>() {

					private static final long serialVersionUID = -4645083522263678220L;

					@Override
					public DocClassifer call(Tuple2<Integer, String> document) throws Exception {
					  DocClassifer docClassifier=new DocClassifer();
					  docClassifier=ut.getDocClassification(document,uniqueVocabPartition1,uniqueVocabPartition2, listLabelProb);
						return docClassifier;
					}
				});
				
				docClassfierRDD1.saveAsTextFile(args[3]);
				System.out.println("Partition done");
			}
			
			break;
		default:
			System.out.println("Please specify correct mode: 'LEARNING' or 'TESTING'");
			break;
		}
		}
	
	
	
	public static Dataset<Row> getCCATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyCCAT) as sumFrequencyCCAT from ccatWordsDF");
		return sumFrequency;
	}
	public static Dataset<Row> getECATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyECAT) as sumFrequencyECAT from ecatWordsDF");
		return sumFrequency;
	}
	public static Dataset<Row> getMCATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyMCAT) as sumFrequencyMCAT from mcatWordsDF");
		return sumFrequency;
	}
	public static Dataset<Row> getGCATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyGCAT) as sumFrequencyGCAT from gcatWordsDF");
		return sumFrequency;
	}
	
	public static Dataset<Row> ccatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> ccatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) AS frequencyCCAT,a1.uniqueword FROM wordCountView a1 "
				+ " WHERE (a1.label_1='CCAT' OR a1.label_2='CCAT' OR a1.label_3='CCAT' OR a1.label_4='CCAT') GROUP BY a1.uniqueword");
	
	     return ccatWordDF;
	}
	
	public static Dataset<Row> ecatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> ecatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) as frequencyECAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='ECAT' OR a1.label_2='ECAT' OR a1.label_3='ECAT' OR a1.label_4='ECAT') GROUP BY a1.uniqueword");
	
	   
	     return ecatWordDF;
	}
	
	public static Dataset<Row> gcatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> gcatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) as frequencyGCAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='GCAT' OR a1.label_2='GCAT' OR a1.label_3='GCAT' OR a1.label_4='GCAT') GROUP BY a1.uniqueword");
	
	  
	     return gcatWordDF;
	}
	public static Dataset<Row> mcatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> mcatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) as frequencyMCAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='MCAT' OR a1.label_2='MCAT' OR a1.label_3='MCAT' OR a1.label_4='MCAT') GROUP BY a1.uniqueword");
	
	    
	     return mcatWordDF;
	}
	
   
	 public static long calculateCCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='CCAT' or label_2='CCAT' or label_3='CCAT' or label_4='CCAT' ");
	     return docId.count();
	 }
	 
	 public static long calculateECATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='ECAT' or label_2='ECAT' or label_3='ECAT' or label_4='ECAT' ");
	     return docId.count();
	 }
	 
	 public static long calculateMCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='MCAT' or label_2='MCAT' or label_3='MCAT' or label_4='MCAT' ");
	     return docId.count();
	 }
	 public static long calculateGCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='GCAT' or label_2='GCAT' or label_3='GCAT' or label_4='GCAT' ");
	     return docId.count();
	 }
	 
 public static Dataset<Row> createPartion(SQLContext sqlCtx,SparkSession sparkSession,long lowerlimt,long upperlimit){
		Dataset<Row> partioned=sqlCtx.sql("SELECT a.uniqueWord,a.probabilityCCAT,a.probabilityECAT,a.probabilityMCAT,a.probabilityGCAT FROM testingVocabProbDFView a where a.uniqueWordId BETWEEN "+lowerlimt +" AND " +upperlimit);
		return partioned;
	}
}

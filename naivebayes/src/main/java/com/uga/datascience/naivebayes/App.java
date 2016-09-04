/*
 * This is main class for Document Classification. 
 * The program works in two different modes (LEARNING and TESTING) that are specified during the execution of the program though command line arguments.
LEARNING:-takes 4 command line arguments:-
args[0]- path of the trainer file
args[1]- path of the classifer file (Classifed lables)
args[2]- path to store the vocab probability file
args[3]- path to store the lable probability file
args[4]- LEARNING 

TESTING:-takes 4 command line arguments:- 
args[0]- path of the test file
args[1]- path of vocab probablity file that was generated using the LEARNING mode
args[2]- path of lable probablity file that was generated using the LEARNING mode
args[3]- path to store the final file that contains the classification for every document
args[4]- TESTING
 */
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

import com.uga.datascience.naivebayes.beans.DocClassifer;
import com.uga.datascience.naivebayes.beans.DocClassifierPair;
import com.uga.datascience.naivebayes.beans.LabelProbability;
import com.uga.datascience.naivebayes.beans.VocabProbability;
import com.uga.datascience.naivebayes.beans.WordCount;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class App.
 */
public class App {
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		// Setting of Spark Configuration
		//System.setProperty("hadoop.home.dir", "C:\\winutils-master\\hadoop-2.7.1");
     	SparkConf conf = new SparkConf().setAppName("App").setMaster("local").set("spark.sql.warehouse.dir", "/spark-warehouse");
		SparkContext ctx=new SparkContext(conf);
		SparkSession sparkSession=SparkSession.builder().appName("NaiveBayes").config(conf).getOrCreate();
		SQLContext sqlCtx=sparkSession.sqlContext();
		// Accumulator for Unique Word and Document Count
		LongAccumulator documentCount=ctx.longAccumulator();
		LongAccumulator vocabuloryCount=ctx.longAccumulator();
	//Pre=Processing of Documents	
		JavaRDD<String> textfile = sparkSession.read().textFile(args[0]).javaRDD();
		Properties prop = Utilities.loadStopWords("stopwords.properties");
		String stop = prop.getProperty("stopwords");	
		Pattern stopwordsPattern = Pattern.compile("\\b(" + stop + ")\\b\\s?", Pattern.CASE_INSENSITIVE);
		String stop1 = prop.getProperty("stopwords1");	
		Pattern stopwordsPattern1 = Pattern.compile("\\b("+stop1 +")\\b\\s?", Pattern.CASE_INSENSITIVE);
		System.out.println(stop);
		JavaRDD<String> rddY = textfile.map(new Function<String, String>() {
			private static final long serialVersionUID = -7726978724451251690L;

			@Override
			public String call(String string) throws Exception {
			String temp = string.toLowerCase().replaceAll(" +", " ");
			String removeSpecial=temp.replaceAll("&(quot;)","").replaceAll("&(amps)", "").replaceAll("[:;,.+/{}()&$*%'?=!]", "").replaceAll("\\b\\d{1,9}\\b\\s?","").replaceAll("\\b\\w{1,3}\\b","").trim();
			Matcher m = stopwordsPattern.matcher(removeSpecial);
			String processedText = m.replaceAll(" ");
			
			String removeHyphen=processedText.replaceAll("-{1,2}", " ").replaceAll(" +", " ").trim();
			String document=removeHyphen.replaceAll("\\w*\\d\\w*", "").trim();
			return document;
			}

		});
		
		JavaRDD<String> rddF = rddY.map(new Function<String, String>() {
			private static final long serialVersionUID = -7726978724451251690L;

			@Override
			public String call(String string) throws Exception {
				Matcher m = stopwordsPattern1.matcher(string);
				String processedText = m.replaceAll("");
				return processedText;	
			}

		});
// Pre-Processing Documents ends
		
		Utilities ut=new Utilities();
		JavaPairRDD<Long, String> linePair=ut.makeLinePair(rddY);
		long docCount=linePair.count();
		documentCount.add(linePair.count());
// Choose of action i.e LERANING/TESTING
		System.out.println("Execution Mode:"+args[4]);
		switch (args[4]) {
		case "LEARNING":
			// LEARNING Starting
			System.out.println("LEARNING Starts");
			// Building Unique Words from training data set	
			JavaRDD<String> uniqueVocab=Utilities.buildVocabulary(rddF); 
			
			vocabuloryCount.add(uniqueVocab.count());
		//Reading Training Label File
			JavaRDD<String> labelFile = sparkSession.read().textFile(args[1]).javaRDD();
	 // Pairing of Training Document and Labels	    
			JavaRDD<String[]> filteredLabelFile=Utilities.getlable(labelFile);
		    JavaPairRDD<Long, String[]> pairedLabel=ut.makeLabelPair(filteredLabelFile);
		 
		    JavaPairRDD<Long, Tuple2<String, String[]>> docClasificationPair=linePair.join(pairedLabel);	   
		    JavaRDD<DocClassifierPair> docClasfierPairRDD=docClasificationPair.map(new Function<Tuple2<Long,Tuple2<String,String[]>>, DocClassifierPair>() {
				private static final long serialVersionUID = 6694664017226133614L;
				@Override
				public DocClassifierPair call(Tuple2<Long, Tuple2<String, String[]>> input) throws Exception {
							  
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
			docClasfierPairDF.createOrReplaceTempView("docClasfierPairDF"); // DataFrame View
			int LIMIT=350000;
		    long sortedCount=docClasificationPair.count();
		    boolean sortedFlag=false;
		    // Code to handle large size of file, using Spark Filter data has been split into partition
		    if(sortedCount>LIMIT){
		    JavaPairRDD<Long, Tuple2<String, String[]>> rddFilter1=docClasificationPair.filter(new Function<Tuple2<Long,Tuple2<String,String[]>>, Boolean>() {

				private static final long serialVersionUID = -8348253772023530419L;

				@Override
				public Boolean call(Tuple2<Long, Tuple2<String, String[]>> arg0) throws Exception {
					// TODO Auto-generated method stub
					return arg0._1().longValue()>=1 &&arg0._1().longValue()<=LIMIT;
				}
			});
		    JavaPairRDD<Long, Tuple2<String, String[]>> rddFilter2=docClasificationPair.filter(new Function<Tuple2<Long,Tuple2<String,String[]>>, Boolean>() {
				private static final long serialVersionUID = -3293638219587414715L;

				@Override
				public Boolean call(Tuple2<Long, Tuple2<String, String[]>> arg0) throws Exception {
					// TODO Auto-generated method stub
					return arg0._1().longValue()>LIMIT &&arg0._1().longValue()<=sortedCount;
				}
			});
		    
		    System.out.println("Entered sortedFlag Partition");
	    	
	    	List<Tuple2<Long, Tuple2<String, String[]>>> sortedCollectionList1=rddFilter1.collect();
	    	System.out.println("sortedCollectionList1"+sortedCollectionList1.size());
	        List<Tuple2<Long, Tuple2<String, String[]>>> sortedCollectionList2=rddFilter2.collect();
	        System.out.println("sortedCollectionList2"+sortedCollectionList2.size());
	        JavaRDD<List<WordCount>> wordCountRDD1=uniqueVocab.map(new Function<String, List<WordCount>>() {
					private static final long serialVersionUID = 7201909674505781255L;

					@Override
					public List<WordCount> call(String line) throws Exception {
						
						List<WordCount> wordCountList=ut.getWordCountList(line, sortedCollectionList1);
						return wordCountList;
					}
				});
	    	 JavaRDD<List<WordCount>> wordCountRDD2=uniqueVocab.map(new Function<String, List<WordCount>>() {
					private static final long serialVersionUID = 7201909674505781255L;

					@Override
					public List<WordCount> call(String line) throws Exception {
						
						List<WordCount> wordCountList=ut.getWordCountList(line, sortedCollectionList2);
						return wordCountList;
					}
				});

				JavaRDD<WordCount> temp1= wordCountRDD1.flatMap(new FlatMapFunction<List<WordCount>, WordCount>() {

						private static final long serialVersionUID = -8395782926696124478L;

						@Override
						public Iterator<WordCount> call(List<WordCount> input) throws Exception {
							return input.iterator();
						}
					});
				JavaRDD<WordCount> temp2= wordCountRDD2.flatMap(new FlatMapFunction<List<WordCount>, WordCount>() {

					private static final long serialVersionUID = -8395782926696124478L;

					@Override
					public Iterator<WordCount> call(List<WordCount> input) throws Exception {
						return input.iterator();
					}
				});
		
				JavaRDD<WordCount> temp=temp1.union(temp2);
				System.out.println("temp: "+temp.count());
				Dataset<Row> wordCountDF=sparkSession.createDataFrame(temp, WordCount.class);
				wordCountDF.createOrReplaceTempView("wordCountView");
				 sortedFlag=true;
		    
		   }
		    
		    
		    if(!sortedFlag){
		    	System.out.println("Document Range in Limit");
			List<Tuple2<Long, Tuple2<String, String[]>>> sortedCollectionList=docClasificationPair.collect();
			
		    JavaRDD<List<WordCount>> wordCountRDD=uniqueVocab.map(new Function<String, List<WordCount>>() {
				private static final long serialVersionUID = 7201909674505781255L;

				@Override
				public List<WordCount> call(String line) throws Exception {
					
					List<WordCount> wordCountList=ut.getWordCountList(line, sortedCollectionList);
					return wordCountList;
				}
			});
			;
			JavaRDD<WordCount> temp= wordCountRDD.flatMap(new FlatMapFunction<List<WordCount>, WordCount>() {

					private static final long serialVersionUID = -8395782926696124478L;

					@Override
					public Iterator<WordCount> call(List<WordCount> input) throws Exception {
						return input.iterator();
					}
				});

			Dataset<Row> wordCountDF=sparkSession.createDataFrame(temp, WordCount.class);
			wordCountDF.createOrReplaceTempView("wordCountView"); // DataFrame 
		    }
		    //Code for calculation for number of unique words occurrence under respective classification in each document
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
			//Code for Calculation of Label Probability i.e number of each classification in training file.
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
			
			// End of Learning
			System.out.println("Learning ended succesfully, ready for testing data set");
				break;
  
	case "TESTING":
		    System.out.println("TESTING starts");
			final long THERSHOLD=750000;
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
			//Code for handling large testing data set
			long vocabProbDFCount =vocabProbDF.count();
			if(vocabProbDFCount<THERSHOLD){
				flag=true;
			}
			if(flag){
				
				Dataset<Row> partition=createPartion(sqlCtx, sparkSession, 1, vocabProbDFCount);
				List<Row> uniqueVocabPartition=partition.collectAsList();
				JavaRDD<DocClassifer> docClassfierRDD=linePair.map(new Function<Tuple2<Long,String>, DocClassifer>() {

					private static final long serialVersionUID = -4645083522263678220L;

					@Override
					public DocClassifer call(Tuple2<Long, String> document) throws Exception {
					  DocClassifer docClassifier=new DocClassifer();
					  docClassifier=ut.getDocClassification(document,uniqueVocabPartition, listLabelProb);
						return docClassifier;
					}
				});
				docClassfierRDD.saveAsTextFile(args[3]);
				flag=true;
			}
			if(!flag){
				
				Dataset<Row> partition1=createPartion(sqlCtx, sparkSession, 1, THERSHOLD);
				Dataset<Row> partition2=createPartion(sqlCtx, sparkSession, THERSHOLD+1, vocabProbDFCount);
				List<Row> uniqueVocabPartition1=partition1.collectAsList();
				List<Row> uniqueVocabPartition2=partition2.collectAsList();
				JavaRDD<DocClassifer> docClassfierRDD1=linePair.map(new Function<Tuple2<Long,String>, DocClassifer>() {

					private static final long serialVersionUID = -4645083522263678220L;

					@Override
					public DocClassifer call(Tuple2<Long, String> document) throws Exception {
					  DocClassifer docClassifier=new DocClassifer();
					  docClassifier=ut.getDocClassification(document,uniqueVocabPartition1,uniqueVocabPartition2, listLabelProb);
						return docClassifier;
					}
				});
				
				docClassfierRDD1.saveAsTextFile(args[3]);
				System.out.println("Partition done");
			}
			//End of Testing 
			System.out.println("Verify your accuracy of document classfication, stored at location: "+args[3]);
			break;
		default:
			System.out.println("Please specify correct mode: 'LEARNING' or 'TESTING'");
			break;

		}
		
	

	}
	
	/**
	 * Gets the CCAT sum.
	 *
	 * @param ctx the ctx
	 * @param session the session
	 * @return the CCAT sum
	 */
	public static Dataset<Row> getCCATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyCCAT) as sumFrequencyCCAT from ccatWordsDF");
		return sumFrequency;
	}
	
	/**
	 * Gets the ECAT sum.
	 *
	 * @param ctx the ctx
	 * @param session the session
	 * @return the ECAT sum
	 */
	public static Dataset<Row> getECATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyECAT) as sumFrequencyECAT from ecatWordsDF");
		return sumFrequency;
	}
	
	/**
	 * Gets the MCAT sum.
	 *
	 * @param ctx the ctx
	 * @param session the session
	 * @return the MCAT sum
	 */
	public static Dataset<Row> getMCATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyMCAT) as sumFrequencyMCAT from mcatWordsDF");
		return sumFrequency;
	}
	
	/**
	 * Gets the GCAT sum.
	 *
	 * @param ctx the ctx
	 * @param session the session
	 * @return the GCAT sum
	 */
	public static Dataset<Row> getGCATSum(SQLContext ctx,SparkSession session){
		Dataset<Row> sumFrequency=ctx.sql("SELECT SUM(frequencyGCAT) as sumFrequencyGCAT from gcatWordsDF");
		return sumFrequency;
	}
	
	/**
	 * Ccat words.
	 *
	 * @param sqlCtx the sql ctx
	 * @param session the session
	 * @return the dataset
	 */
	public static Dataset<Row> ccatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> ccatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) AS frequencyCCAT,a1.uniqueword FROM wordCountView a1 "
				+ " WHERE (a1.label_1='CCAT' OR a1.label_2='CCAT' OR a1.label_3='CCAT' OR a1.label_4='CCAT') GROUP BY a1.uniqueword");
	
	     return ccatWordDF;
	}
	
	/**
	 * Ecat words.
	 *
	 * @param sqlCtx the sql ctx
	 * @param session the session
	 * @return the dataset
	 */
	public static Dataset<Row> ecatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> ecatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) as frequencyECAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='ECAT' OR a1.label_2='ECAT' OR a1.label_3='ECAT' OR a1.label_4='ECAT') GROUP BY a1.uniqueword");
	
	   
	     return ecatWordDF;
	}
	
	/**
	 * Gcat words.
	 *
	 * @param sqlCtx the sql ctx
	 * @param session the session
	 * @return the dataset
	 */
	public static Dataset<Row> gcatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> gcatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) as frequencyGCAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='GCAT' OR a1.label_2='GCAT' OR a1.label_3='GCAT' OR a1.label_4='GCAT') GROUP BY a1.uniqueword");
	
	  
	     return gcatWordDF;
	}
	
	/**
	 * Mcat words.
	 *
	 * @param sqlCtx the sql ctx
	 * @param session the session
	 * @return the dataset
	 */
	public static Dataset<Row> mcatWords(SQLContext sqlCtx, SparkSession session){
		Dataset<Row> mcatWordDF=sqlCtx.sql("SELECT SUM(a1.frequency) as frequencyMCAT,a1.uniqueword FROM wordCountView a1"
				+ " WHERE (a1.label_1='MCAT' OR a1.label_2='MCAT' OR a1.label_3='MCAT' OR a1.label_4='MCAT') GROUP BY a1.uniqueword");
	
	    
	     return mcatWordDF;
	}
	
   
	 /**
 	 * Calculate CCAT count.
 	 *
 	 * @param sqlCtx the sql ctx
 	 * @param sparkSession the spark session
 	 * @return the long
 	 */
 	public static long calculateCCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='CCAT' or label_2='CCAT' or label_3='CCAT' or label_4='CCAT' ");
	     return docId.count();
	 }
	 
	 /**
 	 * Calculate ECAT count.
 	 *
 	 * @param sqlCtx the sql ctx
 	 * @param sparkSession the spark session
 	 * @return the long
 	 */
 	public static long calculateECATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='ECAT' or label_2='ECAT' or label_3='ECAT' or label_4='ECAT' ");
	     return docId.count();
	 }
	 
	 /**
 	 * Calculate MCAT count.
 	 *
 	 * @param sqlCtx the sql ctx
 	 * @param sparkSession the spark session
 	 * @return the long
 	 */
 	public static long calculateMCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='MCAT' or label_2='MCAT' or label_3='MCAT' or label_4='MCAT' ");
	     return docId.count();
	 }
	 
 	/**
 	 * Calculate GCAT count.
 	 *
 	 * @param sqlCtx the sql ctx
 	 * @param sparkSession the spark session
 	 * @return the long
 	 */
 	public static long calculateGCATCount(SQLContext sqlCtx, SparkSession sparkSession){
		 Dataset<Row> docId=sqlCtx.sql("SELECT docId FROM docClasfierPairDF where label_1='GCAT' or label_2='GCAT' or label_3='GCAT' or label_4='GCAT' ");
	     return docId.count();
	 }
	 
 /**
  * Creates the partion.
  *
  * @param sqlCtx the sql ctx
  * @param sparkSession the spark session
  * @param lowerlimt the lowerlimt
  * @param upperlimit the upperlimit
  * @return the dataset
  */
 public static Dataset<Row> createPartion(SQLContext sqlCtx,SparkSession sparkSession,long lowerlimt,long upperlimit){
		Dataset<Row> partioned=sqlCtx.sql("SELECT a.uniqueWord,a.probabilityCCAT,a.probabilityECAT,a.probabilityMCAT,a.probabilityGCAT FROM testingVocabProbDFView a where a.uniqueWordId BETWEEN "+lowerlimt +" AND " +upperlimit);
		return partioned;
	}
}

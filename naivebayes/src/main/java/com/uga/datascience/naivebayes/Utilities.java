package com.uga.datascience.naivebayes;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.uga.datascience.naivebayes.beans.LabelProbability;
import com.uga.datascience.naivebayes.beans.VocabProbability;
import com.uga.datascience.naivebayes.beans.WordCount;

import scala.Tuple2;

public class Utilities implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2555370253381427028L;
	private  int k=0;
	private  int j=0;
	WordCount wordCount=null;
	VocabProbability vocabProbability=null;
	 LabelProbability labelProb=null;
	
	public static Properties loadStopWords(String propertiesFileName) throws Exception{
		Properties prop=new Properties();
		InputStream inputStream=Utilities.class.getResourceAsStream(propertiesFileName);
		if(inputStream!=null){
			prop.load(inputStream);
		}else{
			throw new FileNotFoundException("Failed to load stopword property file:"+ propertiesFileName );
		}
		
		return prop;
	}

	public  JavaPairRDD<Integer, String> makeLinePair(JavaRDD<String> filteredFile){
	//	Utilities.k=0;
		PairFunction<String, Integer, String> keyData=new PairFunction<String, Integer, String>() {
			
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -705423303484558872L;

			@Override
			public Tuple2<Integer, String> call(String fileteredLine) throws Exception {
			//System.out.println(k);
				return new Tuple2<Integer, String>(++k,fileteredLine);
				
			}
		};
		
		JavaPairRDD<Integer, String> linePair=filteredFile.mapToPair(keyData);
		
		return linePair;
	}
	
	public  JavaPairRDD<Integer, String[]> makeLabelPair(JavaRDD<String[]> filteredLabel){
		//Utilities.j=0;
		PairFunction<String[], Integer, String[]> keyData=new PairFunction<String[], Integer, String[]>() {
			
			private static final long serialVersionUID = 7313896833000330630L;

			@Override
			public Tuple2<Integer, String[]> call(String[] fileteredLine) throws Exception {
			
				return new Tuple2<Integer, String[]>(++j,fileteredLine);
				
			}
		};
		
		JavaPairRDD<Integer, String[]> linePair=filteredLabel.mapToPair(keyData);
		return linePair;
	}
	
	public static JavaRDD<String> buildVocabulary(JavaRDD<String> filterdFile){
		JavaRDD<String> vocab=filterdFile.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = -275891577642674322L;

			@Override
			public Iterator<String> call(String filteredLine) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(filteredLine.split(" ")).iterator();
			}
		});
		
		JavaRDD<String> uniqueVocab=vocab.distinct();
		return uniqueVocab;
	}
	
	public static JavaRDD<String[]> getlable(JavaRDD<String> labelFile){
		JavaRDD<String[]> label = labelFile.map(new Function<String, String[]>() {
		private static final long serialVersionUID = 1L;

		@Override
		public String [] call(String labelLine) throws Exception {
		String[] temp=labelLine.split(",");
		List<String> tempList=new ArrayList<String>();
		for(String string:temp){
		if(string.contains("CAT")){
			tempList.add(string);
			}
		}
		return tempList.toArray(new String[tempList.size()]); 
		}

		});
		return label;
		}

 public List<WordCount> getWordCountList(String uniqueWord,List<Tuple2<Integer, Tuple2<String, String[]>>> pairedDocLabel){
     ArrayList<WordCount> wordCountList=new ArrayList<WordCount>(); 
    
	 for(Tuple2<Integer, Tuple2<String, String[]>> input:pairedDocLabel) {
		 
		 if(input._2()._1.contains(uniqueWord)){
			 long frequency=0;	
				wordCount=new WordCount();
				String[] tempArray=input._2()._1.split(" ");
				for(String string:tempArray){
					if(string.equalsIgnoreCase(uniqueWord)){
						++frequency;
					}
				}
				wordCount.setFrequency(frequency);
				if(input._2()._2.length==1){
					wordCount.setLabel_1(input._2()._2[0]);
				}
				if(input._2()._2.length==2){
					wordCount.setLabel_1(input._2()._2[0]);
					wordCount.setLabel_2(input._2()._2[1]);
				}
				if(input._2()._2.length==3){
					wordCount.setLabel_1(input._2()._2[0]);
					wordCount.setLabel_2(input._2()._2[1]);
					wordCount.setLabel_3(input._2()._2[2]);
				}
				if(input._2()._2.length==4){
					wordCount.setLabel_1(input._2()._2[0]);
					wordCount.setLabel_2(input._2()._2[1]);
					wordCount.setLabel_3(input._2()._2[2]);
					wordCount.setLabel_4(input._2()._2[3]);
				}
					
				wordCount.setUniqueword(uniqueWord);	
				wordCount.setDocId(String.valueOf(input._1().intValue()));
				wordCountList.add(wordCount);
			}
				
		}

	
	 return wordCountList;
	 
 }
 
 public VocabProbability getVocabProbability(String uniqueWord,List<Row> ccatWordsDFList,List<Row> ecatWordsDFList,
		 List<Row> mcatWordsDFList,List<Row> gcatWordsDFList,
		 long vocabCount,long ccatWordsDFCount,long ecatWordsDFCount,
		 long mcatWordsDFCount,long gcatWordsDFCount )
 {
	 
	  vocabProbability=new VocabProbability();
	 vocabProbability.setUniqueWord(uniqueWord);
	 for(Row row:ccatWordsDFList)
	 {		 
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
			 double wordProbilityCCAT=calculateProbality(row.getLong(0), ccatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityCCAT(wordProbilityCCAT);
		 }
		 else{
			 double wordProbilityCCAT=calculateProbality(0, ccatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityCCAT(wordProbilityCCAT);
		 }
	 }
	 for(Row row:ecatWordsDFList){
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
			 double wordProbilityECAT=calculateProbality(row.getLong(0), ecatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityECAT(wordProbilityECAT);
		 }
		 else{
			 double wordProbilityECAT=calculateProbality(0, ecatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityECAT(wordProbilityECAT);
		 }
	 }
	 for(Row row:mcatWordsDFList){
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
			 double wordProbilityMCAT=calculateProbality(row.getLong(0), mcatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityMCAT(wordProbilityMCAT);
		 }
		 else{
			 double wordProbilityMCAT=calculateProbality(0, mcatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityMCAT(wordProbilityMCAT);
		 }
	 }
	 for(Row row:gcatWordsDFList){
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
			 double wordProbilityGCAT=calculateProbality(row.getLong(0), gcatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityGCAT(wordProbilityGCAT);
		 }
		 else{
			 double wordProbilityGCAT=calculateProbality(0, gcatWordsDFCount, vocabCount);
			 vocabProbability.setProbabilityGCAT(wordProbilityGCAT);
		 }
	 }
	 
	 
	 
	 return vocabProbability;
	 
 }
 
 public double calculateProbality(long numOccurence,long totalNum,long vocabCount ){
	// System.out.println(numOccurence);
	 double wordUnderLabelProb= Math.log( ((Double.valueOf(numOccurence)+1)/(Double.valueOf(totalNum)+vocabCount)));
	 return wordUnderLabelProb;
 }


 
 public LabelProbability calculateLabelProbability(long docCount,long ccatCount,long ecatCount,long mcatCount,long gcatCount ){
 
 	 double ccatCountProb=Math.log((Double.valueOf(ccatCount)/Double.valueOf(docCount)));
	 double ecatCountProb=Math.log((Double.valueOf(ecatCount)/Double.valueOf(docCount)));
	 double mcatCountProb=Math.log((Double.valueOf(mcatCount)/Double.valueOf(docCount)));
	 double gcatCountProb=Math.log((Double.valueOf(gcatCount)/Double.valueOf(docCount)));
	 labelProb=new LabelProbability();
	 labelProb.setCcatLabelProb(ccatCountProb);
	 labelProb.setEcatLabelProb(ecatCountProb);
	 labelProb.setMcatLabelProb(mcatCountProb);
	 labelProb.setGcatLabelProb(gcatCountProb);
     return labelProb;
	 
 }
}


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
import org.apache.spark.sql.Row;

import com.uga.datascience.naivebayes.beans.DocClassifer;
import com.uga.datascience.naivebayes.beans.LabelProbability;
import com.uga.datascience.naivebayes.beans.VocabProbability;
import com.uga.datascience.naivebayes.beans.WordCount;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class Utilities.
 */
public class Utilities implements Serializable {
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -2555370253381427028L;
	
	/** The k. */
	private  long k=0;
	
	/** The j. */
	private  long j=0;
	
	/** The word count. */
	WordCount wordCount=null;
	
	/** The vocab probability. */
	VocabProbability vocabProbability=null;
	
	/** The label prob. */
	LabelProbability labelProb=null;
	
	/** The doc classifier. */
	DocClassifer docClassifier=null;
	
	/** The probability CCAT. */
	double  probabilityCCAT=0.0;
	  
  	/** The probability ECAT. */
  	double probabilityECAT=0.0;
		
		/** The probability MCAT. */
		double probabilityMCAT=0.0;
		 
 		/** The probability GCAT. */
 		double probabilityGCAT=0.0;
		 
 		/** The probabilities. */
 		double[] probabilities=new double[4];

	/** The label. */
	String label=null;
	 
 	/** The unknown word prob. */
 	double unknownWordProb=.0000000001;
	 
 	/** The max position. */
 	int maxPosition=0;
	
	/**
	 * Load stop words.
	 *
	 * @param propertiesFileName the properties file name
	 * @return the properties
	 * @throws Exception the exception
	 */
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

	/**
	 * Make line pair.
	 *
	 * @param filteredFile the filtered file
	 * @return the java pair RDD
	 */
	public  JavaPairRDD<Long, String> makeLinePair(JavaRDD<String> filteredFile){
		PairFunction<String, Long, String> keyData=new PairFunction<String, Long, String>() {
			private static final long serialVersionUID = -705423303484558872L;

			@Override
			public Tuple2<Long, String> call(String fileteredLine) throws Exception {
				return new Tuple2<Long, String>(++k,fileteredLine);	
			}
		};
		JavaPairRDD<Long, String> linePair=filteredFile.mapToPair(keyData);
		return linePair;
	}
	
	/**
	 * Make label pair.
	 *
	 * @param filteredLabel the filtered label
	 * @return the java pair RDD
	 */
	public  JavaPairRDD<Long, String[]> makeLabelPair(JavaRDD<String[]> filteredLabel){
		PairFunction<String[], Long, String[]> keyData=new PairFunction<String[], Long, String[]>() {
			
			private static final long serialVersionUID = 7313896833000330630L;

			@Override
			public Tuple2<Long, String[]> call(String[] fileteredLine) throws Exception {
			
				return new Tuple2<Long, String[]>(++j,fileteredLine);
				
			}
		};
		JavaPairRDD<Long, String[]> linePair=filteredLabel.mapToPair(keyData);
		return linePair;
	}
	
	/**
	 * Builds the vocabulary.
	 *
	 * @param filterdFile the filterd file
	 * @return the java RDD
	 */
	public static JavaRDD<String> buildVocabulary(JavaRDD<String> filterdFile){
		JavaRDD<String> vocab=filterdFile.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = -275891577642674322L;

			@Override
			public Iterator<String> call(String filteredLine) throws Exception {
				
				
				return Arrays.asList(filteredLine.split(" ")).iterator();
			}
		});
		
		JavaRDD<String> uniqueVocab=vocab.distinct();
		return uniqueVocab;
	}
	
	/**
	 * Gets the lable.
	 *
	 * @param labelFile the label file
	 * @return the lable
	 */
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
   
   /**
    * Gets the lablel prob view.
    *
    * @param list the list
    * @return the lablel prob view
    */
   public List<DocClassifer> getLablelProbView(List<Tuple2<Integer, Tuple2<String, String[]>>> list){
	List<DocClassifer> docClassifierList=new ArrayList<DocClassifer>();
	   for(Tuple2<Integer, Tuple2<String, String[]>> tuple:list){
		   DocClassifer docClasf=new DocClassifer();
		   docClasf.setDocId(String.valueOf(tuple._1()));
		   for(int i=0;i<tuple._2()._2.length;i++){
			   docClasf.setLabel(tuple._2()._2[i]);
			   docClassifierList.add(docClasf);
		   }
		    
	   }
	   return docClassifierList;
   }
 
	
	
	
   /**
    * Gets the word count list.
    *
    * @param uniqueWord the unique word
    * @param pairedDocLabel the paired doc label
    * @return the word count list
    */
   public List<WordCount> getWordCountList(String uniqueWord,List<Tuple2<Long, Tuple2<String, String[]>>> pairedDocLabel){
     ArrayList<WordCount> wordCountList=new ArrayList<WordCount>(); 
    
	 for(Tuple2<Long, Tuple2<String, String[]>> input:pairedDocLabel) {
		 
		 if(input._2()._1.contains(uniqueWord)){
			 long frequency=0;	
				wordCount=new WordCount();
				String[] tempArray=input._2()._1.split(" ");
				for(String string:tempArray){
					if(string.contains(uniqueWord)){
						++frequency;
					}
				}
				wordCount.setFrequency(frequency);
				if(input._2()._2.length==1){
					wordCount.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
				}
				if(input._2()._2.length==2){
					wordCount.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
					wordCount.setLabel_2(input._2()._2[1]!=null?input._2()._2[1].trim():null);
				}
				if(input._2()._2.length==3){
					wordCount.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
					wordCount.setLabel_2(input._2()._2[1]!=null?input._2()._2[1].trim():null);
					wordCount.setLabel_3(input._2()._2[2]!=null?input._2()._2[2].trim():null);
				}
				if(input._2()._2.length==4){
					wordCount.setLabel_1(input._2()._2[0]!=null?input._2()._2[0].trim():null);
					wordCount.setLabel_2(input._2()._2[1]!=null?input._2()._2[1].trim():null);
					wordCount.setLabel_3(input._2()._2[2]!=null?input._2()._2[2].trim():null);
					wordCount.setLabel_4(input._2()._2[3]!=null?input._2()._2[3].trim():null);
				}
					
				wordCount.setUniqueword(uniqueWord);	
				wordCount.setDocId(String.valueOf(input._1().intValue()));
				wordCountList.add(wordCount);
			}
				
		}

	
	 return wordCountList;
	 
 }
 
 /**
  * Gets the vocab probability.
  *
  * @param uniqueWord the unique word
  * @param ccatWordsDFList the ccat words DF list
  * @param ecatWordsDFList the ecat words DF list
  * @param mcatWordsDFList the mcat words DF list
  * @param gcatWordsDFList the gcat words DF list
  * @param vocabCount the vocab count
  * @param ccatWordsDFCount the ccat words DF count
  * @param ecatWordsDFCount the ecat words DF count
  * @param mcatWordsDFCount the mcat words DF count
  * @param gcatWordsDFCount the gcat words DF count
  * @return the vocab probability
  */
 public VocabProbability getVocabProbability(String uniqueWord,List<Row> ccatWordsDFList,List<Row> ecatWordsDFList,
		 List<Row> mcatWordsDFList,List<Row> gcatWordsDFList,
		 long vocabCount,long ccatWordsDFCount,long ecatWordsDFCount,
		 long mcatWordsDFCount,long gcatWordsDFCount )
 {
	 
	 vocabProbability=new VocabProbability();
	 vocabProbability.setUniqueWord(uniqueWord);
	boolean ccatFlag=false;
	double wordProbilityCCAT=0.0;
	boolean ecatFlag=false;
	double wordProbilityECAT=0.0;
	boolean mcatFlag=false;
	double wordProbilityMCAT=0.0;
	boolean gcatFlag=false;
	double wordProbilityGCAT=0.0;
	 for(Row row:ccatWordsDFList)
	 {		 
		 if(row.getString(1).trim().equals(uniqueWord.trim())){
			 wordProbilityCCAT=calculateProbality(row.getLong(0), ccatWordsDFCount, vocabCount,row.getString(1),"CCAT");
			 vocabProbability.setProbabilityCCAT(wordProbilityCCAT);
			 ccatFlag=true;
		 }
	 }
		if(!ccatFlag){
		 wordProbilityCCAT=calculateProbality(0, ccatWordsDFCount, vocabCount,uniqueWord,"CCAT");
		 vocabProbability.setProbabilityCCAT(wordProbilityCCAT);
		}
	 for(Row row:ecatWordsDFList){
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
			 wordProbilityECAT=calculateProbality(row.getLong(0), ecatWordsDFCount, vocabCount,row.getString(1),"ECAT");
			 vocabProbability.setProbabilityECAT(wordProbilityECAT);
			 ecatFlag=true;
		 }
		 
	 }
	 if(!ecatFlag){
			  wordProbilityECAT=calculateProbality(0, ecatWordsDFCount, vocabCount,uniqueWord,"ECAT");
			  vocabProbability.setProbabilityECAT(wordProbilityECAT);
	 }
	 for(Row row:mcatWordsDFList){
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
			 wordProbilityMCAT=calculateProbality(row.getLong(0), mcatWordsDFCount, vocabCount,row.getString(1),"MCAT");
			 vocabProbability.setProbabilityMCAT(wordProbilityMCAT);
			 mcatFlag=true;
		 }
		
	 }
	 if(!mcatFlag){
			 wordProbilityMCAT=calculateProbality(0, mcatWordsDFCount, vocabCount,uniqueWord,"MCAT");
			 vocabProbability.setProbabilityMCAT(wordProbilityMCAT);
	 }
	 for(Row row:gcatWordsDFList){
		 if(row.getString(1)!=null&&row.getString(1).equalsIgnoreCase(uniqueWord)){
		     wordProbilityGCAT=calculateProbality(row.getLong(0), gcatWordsDFCount, vocabCount,row.getString(1),"CCAT");
			 vocabProbability.setProbabilityGCAT(wordProbilityGCAT);
			 gcatFlag=true;
		 }
		 
	 }
	 if(!gcatFlag){
			  wordProbilityGCAT=calculateProbality(0, gcatWordsDFCount, vocabCount,uniqueWord,"CCAT");
			 vocabProbability.setProbabilityGCAT(wordProbilityGCAT);	 
	 }
	 return vocabProbability;
	 
 }
 
 /**
  * Calculate probality.
  *
  * @param numOccurence the num occurence
  * @param totalNum the total num
  * @param vocabCount the vocab count
  * @param word the word
  * @param label the label
  * @return the double
  */
 public double calculateProbality(long numOccurence,long totalNum,long vocabCount,String word,String label ){
	 double wordUnderLabelProb=Math.log( ((Double.valueOf(numOccurence)+1)/(Double.valueOf(totalNum)+vocabCount)));
	 return wordUnderLabelProb;
 }



 /**
  * Calculate label probability.
  *
  * @param docCount the doc count
  * @param ccatCount the ccat count
  * @param ecatCount the ecat count
  * @param mcatCount the mcat count
  * @param gcatCount the gcat count
  * @return the label probability
  */
 public LabelProbability calculateLabelProbability(long docCount,long ccatCount,long ecatCount,long mcatCount,long gcatCount ){
 	 double ccatCountProb=Math.log((Double.valueOf(ccatCount)/docCount));
	 double ecatCountProb=Math.log((Double.valueOf(ecatCount)/docCount));
	 double mcatCountProb=Math.log((Double.valueOf(mcatCount)/docCount));
	 double gcatCountProb=Math.log((Double.valueOf(gcatCount)/docCount));
	 labelProb=new LabelProbability();
	 labelProb.setCcatLabelProb(ccatCountProb);
	 labelProb.setEcatLabelProb(ecatCountProb);
	 labelProb.setMcatLabelProb(mcatCountProb);
	 labelProb.setGcatLabelProb(gcatCountProb);
     return labelProb;
	 
 }
 
 /**
  * Gets the doc classification.
  *
  * @param document the document
  * @param vocabProbList the vocab prob list
  * @param labelProbList the label prob list
  * @return the doc classification
  */
 public DocClassifer getDocClassification(Tuple2<Long, String> document,List<Row> vocabProbList,List<LabelProbability> labelProbList){

	 String[] checkDocument=document._2().split(" "); 
	  
	  probabilityCCAT=labelProbList.get(0).getCcatLabelProb();
	  probabilityECAT=labelProbList.get(0).getEcatLabelProb();
	  probabilityMCAT=labelProbList.get(0).getMcatLabelProb();
	  probabilityGCAT=labelProbList.get(0).getGcatLabelProb();  
	 docClassifier=new DocClassifer();
		 for(String token:checkDocument){
		  boolean flagExist=false;
			 for(Row vocabProb:vocabProbList){	 
				
				 if(token.equals(vocabProb.getString(0))){
				 probabilityCCAT=probabilityCCAT+vocabProb.getDouble(1);
				 probabilityECAT=probabilityECAT+vocabProb.getDouble(2);
				 probabilityMCAT=probabilityMCAT+vocabProb.getDouble(3);
				 probabilityGCAT=probabilityGCAT+vocabProb.getDouble(4);
				 flagExist=true;
			 }	 
		 }	
			 if(!flagExist){
				 probabilityCCAT=probabilityCCAT+Math.log(unknownWordProb);
				 probabilityECAT=probabilityECAT+Math.log(unknownWordProb);
				 probabilityMCAT=probabilityMCAT+Math.log(unknownWordProb);
				 probabilityGCAT=probabilityGCAT+Math.log(unknownWordProb);  
				 }
	 }
		 
	 probabilities[0]=probabilityCCAT;
	 probabilities[1]=probabilityECAT;
	 probabilities[2]=probabilityMCAT;
	 probabilities[3]=probabilityGCAT;
	 label=getLabel(probabilities);
	 docClassifier.setDocId(String.valueOf(document._1()));
	 docClassifier.setLabel(label);
	 return docClassifier;
 }
 
 /**
  * Gets the doc classification.
  *
  * @param document the document
  * @param vocabProbList1 the vocab prob list 1
  * @param vocabProbList2 the vocab prob list 2
  * @param labelProbList the label prob list
  * @return the doc classification
  */
 public DocClassifer getDocClassification(Tuple2<Long, String> document,List<Row> vocabProbList1,List<Row> vocabProbList2,List<LabelProbability> labelProbList){

	 String[] checkDocument=document._2().split(" "); 
	  
	  probabilityCCAT=labelProbList.get(0).getCcatLabelProb();
	  probabilityECAT=labelProbList.get(0).getEcatLabelProb();
	  probabilityMCAT=labelProbList.get(0).getMcatLabelProb();
	  probabilityGCAT=labelProbList.get(0).getGcatLabelProb();  
	  boolean checkInList=false;
	  boolean flagExist=false;
	  docClassifier=new DocClassifer();
		 for(String token:checkDocument){
			 checkInList=false;
			 for(Row vocabProb:vocabProbList1){	 
				 flagExist=false;
				 if(token.equals(vocabProb.getString(0))){
					 probabilityCCAT=probabilityCCAT+vocabProb.getDouble(1);
					 probabilityECAT=probabilityECAT+vocabProb.getDouble(2);
					 probabilityMCAT=probabilityMCAT+vocabProb.getDouble(3);
					 probabilityGCAT=probabilityGCAT+vocabProb.getDouble(4);
					 flagExist=true;
				 }	 
			 }
			 if(!flagExist){
				 for(Row vocabProb1:vocabProbList2){	 
					 checkInList=false;
					 if(token.equals(vocabProb1.getString(0))){
						 probabilityCCAT=probabilityCCAT+vocabProb1.getDouble(1);
						 probabilityECAT=probabilityECAT+vocabProb1.getDouble(2);
						 probabilityMCAT=probabilityMCAT+vocabProb1.getDouble(3);
						 probabilityGCAT=probabilityGCAT+vocabProb1.getDouble(4);
						 checkInList=true;
					 }	 
			 }
			
		 }	
			 if(!checkInList){
				 probabilityCCAT=probabilityCCAT+Math.log(unknownWordProb);
				 probabilityECAT=probabilityECAT+Math.log(unknownWordProb);
				 probabilityMCAT=probabilityMCAT+Math.log(unknownWordProb);
				 probabilityGCAT=probabilityGCAT+Math.log(unknownWordProb);  
				 }
	 }
		 
     probabilities[0]=probabilityCCAT;
	 probabilities[1]=probabilityECAT;
	 probabilities[2]=probabilityMCAT;
	 probabilities[3]=probabilityGCAT;
    
	 label=getLabel(probabilities);
	 docClassifier.setDocId(String.valueOf(document._1()));
	 docClassifier.setLabel(label);
	 return docClassifier;
 }

 
 /**
  * Gets the label.
  *
  * @param probabilities the probabilities
  * @return the label
  */
 public String getLabel(double[] probabilities){
	 int maxPosition=0;
	 String label=null;
	 for(int i=1;i<probabilities.length;i++){
		 double element=probabilities[i];
		 if(element>probabilities[maxPosition]){
			 maxPosition=i;
		 }
	}
	 if(maxPosition==0)
		 label= "CCAT";
	 if(maxPosition==1)
		 label= "ECAT";
	 if(maxPosition==2)
		 label= "MCAT";
	 if(maxPosition==3)
		 label= "GCAT";
	
	 return label;
 }
 
}


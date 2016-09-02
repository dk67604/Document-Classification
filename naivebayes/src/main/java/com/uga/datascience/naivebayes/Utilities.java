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
	DocClassifer docClassifier=null;
	double  probabilityCCAT=0.0;
	  double probabilityECAT=0.0;
		double probabilityMCAT=0.0;
		 double probabilityGCAT=0.0;
		 double[] probabilities=new double[4];

	String label=null;
	 double unknownWordProb=.0000000001;
	 int maxPosition=0;
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
 
	
	
	public List<WordCount> getWordCountList(String uniqueWord,List<Tuple2<Integer, Tuple2<String, String[]>>> pairedDocLabel){
     ArrayList<WordCount> wordCountList=new ArrayList<WordCount>(); 
    
	 for(Tuple2<Integer, Tuple2<String, String[]>> input:pairedDocLabel) {
		 
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
		 if(row.getString(1).trim().contains(uniqueWord.trim())){
			 //System.out.println("Enterd If");
			 //System.out.println("CCAT "+"row.getString(1):"+row.getString(1)+" row.getLong(0):"+row.getLong(0));
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
			// System.out.println("ECAT"+"row.getString(1):"+row.getString(1)+" row.getLong(0):"+row.getLong(0));
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
		//	 System.out.println("MCAT"+"row.getString(1):"+row.getString(1)+" row.getLong(0):"+row.getLong(0));
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
			 //System.out.println("GCAT"+"row.getString(1):"+row.getString(1)+" row.getLong(0):"+row.getLong(0));
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
 
 public double calculateProbality(long numOccurence,long totalNum,long vocabCount,String word,String label ){
	 double wordUnderLabelProb=Math.log( ((Double.valueOf(numOccurence)+1)/(Double.valueOf(totalNum)+vocabCount)));
	 
	 //System.out.println("word: "+word+" label: "+label+"numOccurence:"+numOccurence+ "totalNum: "+totalNum+"vocabCount: "+vocabCount+" wordUnderLabelProb: "+wordUnderLabelProb);
	 //System.out.println("---------------------------------------------------------------------------------");
	 return wordUnderLabelProb;
 }



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
 
 public DocClassifer getDocClassification(Tuple2<Integer, String> document,List<Row> vocabProbList,List<LabelProbability> labelProbList){

	 String[] checkDocument=document._2().split(" "); 
	  
	  probabilityCCAT=labelProbList.get(0).getCcatLabelProb();
	  probabilityECAT=labelProbList.get(0).getEcatLabelProb();
	  probabilityMCAT=labelProbList.get(0).getMcatLabelProb();
	  probabilityGCAT=labelProbList.get(0).getGcatLabelProb();  
	 int i=0;

	 docClassifier=new DocClassifer();
		 for(String token:checkDocument){
		  boolean flagExist=false;
			 //System.out.println("token"+token);
			 for(Row vocabProb:vocabProbList){	 
				
			 if(token.contains(vocabProb.getString(0))){
			//	 System.out.println("Entered If");
				 probabilityCCAT=probabilityCCAT+vocabProb.getDouble(1);
				// System.out.println("probabilityCCAT"+probabilityCCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(1));
				 probabilityECAT=probabilityECAT+vocabProb.getDouble(2);
				// System.out.println("probabilityECAT"+probabilityECAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(2));
				 probabilityMCAT=probabilityMCAT+vocabProb.getDouble(3);
				// System.out.println("probabilityMCAT"+probabilityMCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(3));
				 probabilityGCAT=probabilityGCAT+vocabProb.getDouble(4);
				 //System.out.println("probabilityGCAT"+probabilityGCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(4));
				 flagExist=true;
			
			 }	 
			
			  
		 }	
			 if(!flagExist){
				 
			//	 System.out.println("Entered flag"+ ++i);
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
	//System.out.println(" probabilities:"+ probabilities[0]+" "+ probabilities[1]+" "+ probabilities[2] +" "+  probabilities[3]);
	 label=getLabel(probabilities);
	 docClassifier.setDocId(String.valueOf(document._1()));
	 docClassifier.setLabel(label);
	 return docClassifier;
 }
 
 public DocClassifer getDocClassification(Tuple2<Integer, String> document,List<Row> vocabProbList1,List<Row> vocabProbList2,List<LabelProbability> labelProbList){

	 String[] checkDocument=document._2().split(" "); 
	  
	  probabilityCCAT=labelProbList.get(0).getCcatLabelProb();
	  probabilityECAT=labelProbList.get(0).getEcatLabelProb();
	  probabilityMCAT=labelProbList.get(0).getMcatLabelProb();
	  probabilityGCAT=labelProbList.get(0).getGcatLabelProb();  
	  int i=0;
	  boolean checkInList=false;
	  boolean flagExist=false;
	 docClassifier=new DocClassifer();
		 for(String token:checkDocument){
			 checkInList=false;
			 //System.out.println("token"+token);
			 for(Row vocabProb:vocabProbList1){	 
				 flagExist=false;
			 if(token.contains(vocabProb.getString(0))){
			//	 System.out.println("Entered If");
				 probabilityCCAT=probabilityCCAT+vocabProb.getDouble(1);
				// System.out.println("probabilityCCAT"+probabilityCCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(1));
				 probabilityECAT=probabilityECAT+vocabProb.getDouble(2);
				// System.out.println("probabilityECAT"+probabilityECAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(2));
				 probabilityMCAT=probabilityMCAT+vocabProb.getDouble(3);
				// System.out.println("probabilityMCAT"+probabilityMCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(3));
				 probabilityGCAT=probabilityGCAT+vocabProb.getDouble(4);
				 //System.out.println("probabilityGCAT"+probabilityGCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(4));
				 flagExist=true;
			
			 }	 
			 }
			 if(!flagExist){
				 for(Row vocabProb1:vocabProbList2){	 
					 checkInList=false;
					 if(token.contains(vocabProb1.getString(0))){
					//	 System.out.println("Entered If");
						 probabilityCCAT=probabilityCCAT+vocabProb1.getDouble(1);
						// System.out.println("probabilityCCAT"+probabilityCCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(1));
						 probabilityECAT=probabilityECAT+vocabProb1.getDouble(2);
						// System.out.println("probabilityECAT"+probabilityECAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(2));
						 probabilityMCAT=probabilityMCAT+vocabProb1.getDouble(3);
						// System.out.println("probabilityMCAT"+probabilityMCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(3));
						 probabilityGCAT=probabilityGCAT+vocabProb1.getDouble(4);
						 //System.out.println("probabilityGCAT"+probabilityGCAT+"vocabProb.getDouble(1)"+vocabProb.getDouble(4));
						 checkInList=true;
					
					 }	 
			 }
			
			  
		 }	
			 if(!checkInList){
				 
			//	 System.out.println("Entered flag"+ ++i);
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
	//System.out.println(" probabilities:"+ probabilities[0]+" "+ probabilities[1]+" "+ probabilities[2] +" "+  probabilities[3]);
	 label=getLabel(probabilities);
	 docClassifier.setDocId(String.valueOf(document._1()));
	 docClassifier.setLabel(label);
	 return docClassifier;
 }

 
 public String getLabel(double[] probabilities){
	 int maxPosition=0;
	 String label=null;
	 for(int i=1;i<probabilities.length;i++){
		 double element=probabilities[i];
		 if(element>probabilities[maxPosition]){
			 maxPosition=i;
		 }
	}
//	 System.out.println("maxposition:"+maxPosition);
	 if(maxPosition==0)
		 label= "CCAT";
	 if(maxPosition==1)
		 label= "ECAT";
	 if(maxPosition==2)
		 label= "MCAT";
	 if(maxPosition==3)
		 label= "GCAT";
	// System.out.println(label);
	 return label;
 }
 
}


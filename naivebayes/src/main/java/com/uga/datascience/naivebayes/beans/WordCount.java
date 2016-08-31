package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

/**
 * @author 
 *
 */
public class WordCount implements Serializable {

	private static final long serialVersionUID = -1957301682225690667L;
	private long frequency;
	private String uniqueword;
	private String docId;
	private  String label_1;
	private  String label_2;
	private  String label_3;
	private  String label_4;
	
	public long getFrequency() {
		return frequency;
	}
	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}
	public String getUniqueword() {
		return uniqueword;
	}
	public void setUniqueword(String uniqueword) {
		this.uniqueword = uniqueword;
	}
	public String getDocId() {
		return docId;
	}
	public void setDocId(String docId) {
		this.docId = docId;
	}
	public String getLabel_1() {
		return label_1;
	}
	public void setLabel_1(String label_1) {
		this.label_1 = label_1;
	}
	public String getLabel_2() {
		return label_2;
	}
	public void setLabel_2(String label_2) {
		this.label_2 = label_2;
	}
	public String getLabel_3() {
		return label_3;
	}
	public void setLabel_3(String label_3) {
		this.label_3 = label_3;
	}
	public String getLabel_4() {
		return label_4;
	}
	public void setLabel_4(String label_4) {
		this.label_4 = label_4;
	}
	@Override
	public String toString() {
		return "WordCount [frequency=" + frequency + ", uniqueword=" + uniqueword + ", docId=" + docId + ", label_1="
				+ label_1 + ", label_2=" + label_2 + ", label_3=" + label_3 + ", label_4=" + label_4 + "]";
	}

	
}

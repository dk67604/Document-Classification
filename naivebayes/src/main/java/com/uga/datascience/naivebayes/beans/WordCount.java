package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;


// TODO: Auto-generated Javadoc
/**
 * The Class WordCount.
 */
public class WordCount implements Serializable {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1957301682225690667L;
	
	/** The frequency. */
	private long frequency;
	
	/** The uniqueword. */
	private String uniqueword;
	
	/** The doc id. */
	private String docId;
	
	/** The label 1. */
	private  String label_1;
	
	/** The label 2. */
	private  String label_2;
	
	/** The label 3. */
	private  String label_3;
	
	/** The label 4. */
	private  String label_4;
	
	/**
	 * Gets the frequency.
	 *
	 * @return the frequency
	 */
	public long getFrequency() {
		return frequency;
	}
	
	/**
	 * Sets the frequency.
	 *
	 * @param frequency the new frequency
	 */
	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}
	
	/**
	 * Gets the uniqueword.
	 *
	 * @return the uniqueword
	 */
	public String getUniqueword() {
		return uniqueword;
	}
	
	/**
	 * Sets the uniqueword.
	 *
	 * @param uniqueword the new uniqueword
	 */
	public void setUniqueword(String uniqueword) {
		this.uniqueword = uniqueword;
	}
	
	/**
	 * Gets the doc id.
	 *
	 * @return the doc id
	 */
	public String getDocId() {
		return docId;
	}
	
	/**
	 * Sets the doc id.
	 *
	 * @param docId the new doc id
	 */
	public void setDocId(String docId) {
		this.docId = docId;
	}
	
	/**
	 * Gets the label 1.
	 *
	 * @return the label 1
	 */
	public String getLabel_1() {
		return label_1;
	}
	
	/**
	 * Sets the label 1.
	 *
	 * @param label_1 the new label 1
	 */
	public void setLabel_1(String label_1) {
		this.label_1 = label_1;
	}
	
	/**
	 * Gets the label 2.
	 *
	 * @return the label 2
	 */
	public String getLabel_2() {
		return label_2;
	}
	
	/**
	 * Sets the label 2.
	 *
	 * @param label_2 the new label 2
	 */
	public void setLabel_2(String label_2) {
		this.label_2 = label_2;
	}
	
	/**
	 * Gets the label 3.
	 *
	 * @return the label 3
	 */
	public String getLabel_3() {
		return label_3;
	}
	
	/**
	 * Sets the label 3.
	 *
	 * @param label_3 the new label 3
	 */
	public void setLabel_3(String label_3) {
		this.label_3 = label_3;
	}
	
	/**
	 * Gets the label 4.
	 *
	 * @return the label 4
	 */
	public String getLabel_4() {
		return label_4;
	}
	
	/**
	 * Sets the label 4.
	 *
	 * @param label_4 the new label 4
	 */
	public void setLabel_4(String label_4) {
		this.label_4 = label_4;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "WordCount [frequency=" + frequency + ", uniqueword=" + uniqueword + ", docId=" + docId + ", label_1="
				+ label_1 + ", label_2=" + label_2 + ", label_3=" + label_3 + ", label_4=" + label_4 + "]";
	}

	
}

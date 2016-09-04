package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class VocabProbability.
 */
public class VocabProbability implements Serializable{

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -7391811171481895298L;
	
	/** The unique word. */
	private String uniqueWord;
	
	/** The probability CCAT. */
	private double probabilityCCAT;
	
	/** The probability ECAT. */
	private double probabilityECAT;
	
	/** The probability GCAT. */
	private double probabilityGCAT;
	
	/** The probability MCAT. */
	private double probabilityMCAT;
	
	/** The unique word id. */
	private long uniqueWordId;
	
	/**
	 * Gets the unique word.
	 *
	 * @return the unique word
	 */
	public String getUniqueWord() {
		return uniqueWord;
	}
	
	/**
	 * Sets the unique word.
	 *
	 * @param uniqueWord the new unique word
	 */
	public void setUniqueWord(String uniqueWord) {
		this.uniqueWord = uniqueWord;
	}
	
	/**
	 * Gets the probability CCAT.
	 *
	 * @return the probability CCAT
	 */
	public double getProbabilityCCAT() {
		return probabilityCCAT;
	}
	
	/**
	 * Sets the probability CCAT.
	 *
	 * @param probabilityCCAT the new probability CCAT
	 */
	public void setProbabilityCCAT(double probabilityCCAT) {
		this.probabilityCCAT = probabilityCCAT;
	}
	
	/**
	 * Gets the probability ECAT.
	 *
	 * @return the probability ECAT
	 */
	public double getProbabilityECAT() {
		return probabilityECAT;
	}
	
	/**
	 * Sets the probability ECAT.
	 *
	 * @param probabilityECAT the new probability ECAT
	 */
	public void setProbabilityECAT(double probabilityECAT) {
		this.probabilityECAT = probabilityECAT;
	}
	
	/**
	 * Gets the probability GCAT.
	 *
	 * @return the probability GCAT
	 */
	public double getProbabilityGCAT() {
		return probabilityGCAT;
	}
	
	/**
	 * Sets the probability GCAT.
	 *
	 * @param probabilityGCAT the new probability GCAT
	 */
	public void setProbabilityGCAT(double probabilityGCAT) {
		this.probabilityGCAT = probabilityGCAT;
	}
	
	/**
	 * Gets the probability MCAT.
	 *
	 * @return the probability MCAT
	 */
	public double getProbabilityMCAT() {
		return probabilityMCAT;
	}
	
	/**
	 * Sets the probability MCAT.
	 *
	 * @param probabilityMCAT the new probability MCAT
	 */
	public void setProbabilityMCAT(double probabilityMCAT) {
		this.probabilityMCAT = probabilityMCAT;
	}

	/**
	 * Gets the unique word id.
	 *
	 * @return the unique word id
	 */
	public long getUniqueWordId() {
		return uniqueWordId;
	}
	
	/**
	 * Sets the unique word id.
	 *
	 * @param uniqueWordId the new unique word id
	 */
	public void setUniqueWordId(long uniqueWordId) {
		this.uniqueWordId = uniqueWordId;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "VocabProbability [uniqueWord=" + uniqueWord + ", probabilityCCAT=" + probabilityCCAT
				+ ", probabilityECAT=" + probabilityECAT + ", probabilityGCAT=" + probabilityGCAT + ", probabilityMCAT="
				+ probabilityMCAT + ", uniqueWordId=" + uniqueWordId + "]";
	}
	
	
}

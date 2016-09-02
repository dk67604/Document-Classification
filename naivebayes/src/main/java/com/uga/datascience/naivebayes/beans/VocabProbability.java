package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

public class VocabProbability implements Serializable{

	private static final long serialVersionUID = -7391811171481895298L;
	private String uniqueWord;
	private double probabilityCCAT;
	private double probabilityECAT;
	private double probabilityGCAT;
	private double probabilityMCAT;
	private long uniqueWordId;
	
	public String getUniqueWord() {
		return uniqueWord;
	}
	public void setUniqueWord(String uniqueWord) {
		this.uniqueWord = uniqueWord;
	}
	public double getProbabilityCCAT() {
		return probabilityCCAT;
	}
	public void setProbabilityCCAT(double probabilityCCAT) {
		this.probabilityCCAT = probabilityCCAT;
	}
	public double getProbabilityECAT() {
		return probabilityECAT;
	}
	public void setProbabilityECAT(double probabilityECAT) {
		this.probabilityECAT = probabilityECAT;
	}
	public double getProbabilityGCAT() {
		return probabilityGCAT;
	}
	public void setProbabilityGCAT(double probabilityGCAT) {
		this.probabilityGCAT = probabilityGCAT;
	}
	public double getProbabilityMCAT() {
		return probabilityMCAT;
	}
	public void setProbabilityMCAT(double probabilityMCAT) {
		this.probabilityMCAT = probabilityMCAT;
	}

	public long getUniqueWordId() {
		return uniqueWordId;
	}
	public void setUniqueWordId(long uniqueWordId) {
		this.uniqueWordId = uniqueWordId;
	}
	@Override
	public String toString() {
		return "VocabProbability [uniqueWord=" + uniqueWord + ", probabilityCCAT=" + probabilityCCAT
				+ ", probabilityECAT=" + probabilityECAT + ", probabilityGCAT=" + probabilityGCAT + ", probabilityMCAT="
				+ probabilityMCAT + ", uniqueWordId=" + uniqueWordId + "]";
	}
	
	
}

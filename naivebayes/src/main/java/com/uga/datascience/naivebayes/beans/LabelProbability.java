package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

public class LabelProbability implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5657545931598489869L;
	private double ccatLabelProb;
	private double ecatLabelProb;
	private double mcatLabelProb;
	private double gcatLabelProb;
	
	public double getCcatLabelProb() {
		return ccatLabelProb;
	}
	public void setCcatLabelProb(double ccatLabelProb) {
		this.ccatLabelProb = ccatLabelProb;
	}
	public double getEcatLabelProb() {
		return ecatLabelProb;
	}
	public void setEcatLabelProb(double ecatLabelProb) {
		this.ecatLabelProb = ecatLabelProb;
	}
	public double getMcatLabelProb() {
		return mcatLabelProb;
	}
	public void setMcatLabelProb(double mcatLabelProb) {
		this.mcatLabelProb = mcatLabelProb;
	}
	public double getGcatLabelProb() {
		return gcatLabelProb;
	}
	public void setGcatLabelProb(double gcatLabelProb) {
		this.gcatLabelProb = gcatLabelProb;
	}
	@Override
	public String toString() {
		return "LabelProbability [ccatLabelProb=" + ccatLabelProb + ", ecatLabelProb=" + ecatLabelProb
				+ ", mcatLabelProb=" + mcatLabelProb + ", gcatLabelProb=" + gcatLabelProb + "]";
	}
}

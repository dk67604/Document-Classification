package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class LabelProbability.
 */
public class LabelProbability implements Serializable {
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5657545931598489869L;
	
	/** The ccat label prob. */
	private double ccatLabelProb;
	
	/** The ecat label prob. */
	private double ecatLabelProb;
	
	/** The mcat label prob. */
	private double mcatLabelProb;
	
	/** The gcat label prob. */
	private double gcatLabelProb;
	
	/**
	 * Gets the ccat label prob.
	 *
	 * @return the ccat label prob
	 */
	public double getCcatLabelProb() {
		return ccatLabelProb;
	}
	
	/**
	 * Sets the ccat label prob.
	 *
	 * @param ccatLabelProb the new ccat label prob
	 */
	public void setCcatLabelProb(double ccatLabelProb) {
		this.ccatLabelProb = ccatLabelProb;
	}
	
	/**
	 * Gets the ecat label prob.
	 *
	 * @return the ecat label prob
	 */
	public double getEcatLabelProb() {
		return ecatLabelProb;
	}
	
	/**
	 * Sets the ecat label prob.
	 *
	 * @param ecatLabelProb the new ecat label prob
	 */
	public void setEcatLabelProb(double ecatLabelProb) {
		this.ecatLabelProb = ecatLabelProb;
	}
	
	/**
	 * Gets the mcat label prob.
	 *
	 * @return the mcat label prob
	 */
	public double getMcatLabelProb() {
		return mcatLabelProb;
	}
	
	/**
	 * Sets the mcat label prob.
	 *
	 * @param mcatLabelProb the new mcat label prob
	 */
	public void setMcatLabelProb(double mcatLabelProb) {
		this.mcatLabelProb = mcatLabelProb;
	}
	
	/**
	 * Gets the gcat label prob.
	 *
	 * @return the gcat label prob
	 */
	public double getGcatLabelProb() {
		return gcatLabelProb;
	}
	
	/**
	 * Sets the gcat label prob.
	 *
	 * @param gcatLabelProb the new gcat label prob
	 */
	public void setGcatLabelProb(double gcatLabelProb) {
		this.gcatLabelProb = gcatLabelProb;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "LabelProbability [ccatLabelProb=" + ccatLabelProb + ", ecatLabelProb=" + ecatLabelProb
				+ ", mcatLabelProb=" + mcatLabelProb + ", gcatLabelProb=" + gcatLabelProb + "]";
	}
}

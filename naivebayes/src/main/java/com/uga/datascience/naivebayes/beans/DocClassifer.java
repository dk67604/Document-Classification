package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class DocClassifer.
 */
public class DocClassifer implements Serializable {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -2699734172611548052L;
	
	/** The doc id. */
	private String docId;
    
    /** The label. */
    private String label;
    
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
	 * Gets the label.
	 *
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}
	
	/**
	 * Sets the label.
	 *
	 * @param label the new label
	 */
	public void setLabel(String label) {
		this.label = label;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DocClassifer [docId=" + docId + ", label=" + label + "]";
	}	
    
    
}

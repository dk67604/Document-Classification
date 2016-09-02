package com.uga.datascience.naivebayes.beans;

import java.io.Serializable;

public class DocClassifer implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2699734172611548052L;
	private String docId;
    private String label;
    
	public String getDocId() {
		return docId;
	}
	public void setDocId(String docId) {
		this.docId = docId;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	@Override
	public String toString() {
		return "DocClassifer [docId=" + docId + ", label=" + label + "]";
	}	
    
    
}

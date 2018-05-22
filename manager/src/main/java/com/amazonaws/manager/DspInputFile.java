package com.amazonaws.manager;



public class DspInputFile {
	private String title;
	private DspReview[] reviews;
	
	public DspInputFile(DspInputFile input) {
		this.reviews=input.getReviews();
		this.title=input.getTitle();
	}
	
	
	public DspInputFile(String title,DspReview[] reviews) {
		this.reviews=reviews;
		this.title=title;
	}
	
	public DspReview[] getReviews() {
		return this.reviews;
	}
	
	public String getTitle() {
		return this.title;
	}
	
}









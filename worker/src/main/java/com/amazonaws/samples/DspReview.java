package com.amazonaws.samples;

public class DspReview {
	
	private String id;
	private String link;
	private String title;
	private String text;
	private int rating;
	private String author;
	private String date;
	
	public DspReview(String id, String link, String title, String text, int rating, String author, String date) {
		this.id = id;
		this.link = link;
		this.title = title;
		this.text = text;
		this.rating = rating;
		this.author = author;
		this.date = date;
	}

	public DspReview(DspReview review) {
		
		this.id = review.id;
		this.link = review.link;
		this.title = review.title;
		this.text = review.text;
		this.rating = review.rating;
		this.author = review.author;
		this.date = review.date;
	}

	public String getLink() {
		return link;
	}
	
	public String getText() {
		return text;
	}

	public int getRating() {
		return rating;
	}

	
}

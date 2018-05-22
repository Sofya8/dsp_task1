package com.amazonaws.manager;

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

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

}

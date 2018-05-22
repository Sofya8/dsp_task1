package com.amazonaws.localapp;

public class DspSolution {
	private String review;
	private String link;
	private int rating;
	private String color;
	private String entities;
	private boolean sarcastic;
	private int sentiment;
	
	
	
	public int getSentiment() {
		return sentiment;
	}

	public void setSentiment(int sentiment) {
		this.sentiment = sentiment;
	}
	
	
	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}
	
	
	public String getReview() {
		return review;
	}

	public void setReview(String review) {
		this.review = review;
	}
	

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}
	
	
	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public String getEntities() {
		return entities;
	}

	public void setEntities(String entities) {
		this.entities = entities;
	}

	public boolean getSarcastic() {
		return sarcastic;
	}

	public void setSarcastic(boolean sarcastic) {
		this.sarcastic = sarcastic;
	}

	public DspSolution(String link, String text, int rating,
			String color, String entities, boolean sarcastic, int sentiment) {
		this.sentiment = sentiment;
		this.review=text;
		this.link=link;
		this.rating=rating;
		this.color = color;
		this.entities = entities;
		this.sarcastic = sarcastic;
	}

	public DspSolution( String text, String link, int rating, int sentiment, String entities, boolean sarcastic2) {
		this.sentiment = sentiment;
		this.review = text;
		this.link = link;
		this.rating = rating;
		this.entities = entities;
		this.sarcastic = sarcastic2;
		switch(sentiment) {
			case 0:color="darkred"; 
			case 1:color="red"; 
			case 2:color="black"; 
			case 3:color="lightgreen"; 
			case 4:color="darkgreen"; 
		}
	}
	
	

}

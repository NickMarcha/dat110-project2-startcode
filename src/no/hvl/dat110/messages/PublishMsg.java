package no.hvl.dat110.messages;

public class PublishMsg extends Message {
	
	// message sent from client to create publish a message on a topic 

	// TODO:
	// Implement object variables - a topic and a message is required

	private String topic;
	private String message;
	
	// Constructor, get/set-methods, and toString method
    // as described in the project text	
	
	/**
	 * @param user
	 * @param topic
	 */
	public PublishMsg(String user, String topic, String message) {
		super(MessageType.PUBLISH,user);
		this.topic = topic;
		this.message = message;
	}
	
	@Override
	public String toString() {
		return ("PublishMsg [topic = "+topic+", message = "+message+"]") + super.toString();
		
	}
	
	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public String getMessage() {
		return message;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}
}

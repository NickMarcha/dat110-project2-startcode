package no.hvl.dat110.messages;

public class DeleteTopicMsg extends Message {

	// message sent from client to create topic on the broker

	// TODO:
	// Implement object variables - a topic is required

	// Constructor, get/set-methods, and toString method
	// as described in the project text
	
private String topic;
	
	// Constructor, get/set-methods, and toString method
    // as described in the project text	
	
	/**
	 * @param user
	 * @param topic
	 */
	public DeleteTopicMsg(String user, String topic) {
		super(MessageType.DELETETOPIC,user);
		this.topic = topic;
	}
	
	@Override
	public String toString() {
		return ("DeleteTopicMsg [topic = "+topic+"]") + super.toString();
		
	}

	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}

}

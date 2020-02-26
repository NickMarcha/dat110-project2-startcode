package no.hvl.dat110.broker;

import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Collection;
import java.util.Queue;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher extends Stopable {

	private ClientSession client;
	private Storage storage;

	public Dispatcher(Storage storage, ClientSession client) {
		super("Dispatcher");
		this.storage = storage;
		this.client = client;

	}

	@Override
	public void doProcess() {

		//Collection<ClientSession> clients = storage.getSessions();

		Logger.lg(".");
		/*
		for (ClientSession client : clients) {
		 */
		
		Message msg = null;

		if (client.hasData()) {
			msg = client.receive();
		}

		// a message was received
		if (msg != null) {
			dispatch(client, msg);
		}
		/*
		}
		 */

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void dispatch(ClientSession client, Message msg) {

		MessageType type = msg.getType();

		// invoke the appropriate handler method
		switch (type) {

		case DISCONNECT:
			onDisconnect((DisconnectMsg) msg);
			break;

		case CREATETOPIC:
			onCreateTopic((CreateTopicMsg) msg);
			break;

		case DELETETOPIC:
			onDeleteTopic((DeleteTopicMsg) msg);
			break;

		case SUBSCRIBE:
			onSubscribe((SubscribeMsg) msg);
			break;

		case UNSUBSCRIBE:
			onUnsubscribe((UnsubscribeMsg) msg);
			break;

		case PUBLISH:
			onPublish((PublishMsg) msg);
			break;

		default:
			Logger.log("broker dispatch - unhandled message type");
			break;

		}
	}

	// called from Broker after having established the underlying connection
	public void onConnect(ConnectMsg msg, Connection connection) {

		String user = msg.getUser();

		Logger.log("onConnect:" + msg.toString());

		storage.addClientSession(user, connection);

		if(storage.bufferedMessages.containsKey(user)) {
			Queue<PublishMsg> bufferedMessages = storage.bufferedMessages.get(user);

			ClientSession CS = storage.getSession(user);

			bufferedMessages.forEach(bMsg -> CS.send(bMsg));

			storage.bufferedMessages.remove(user);
		}

	}

	// called by dispatch upon receiving a disconnect message
	public void onDisconnect(DisconnectMsg msg) {

		String user = msg.getUser();

		Logger.log("onDisconnect:" + msg.toString());

		storage.removeClientSession(user);
		doStop();

	}

	public void onCreateTopic(CreateTopicMsg msg) {

		Logger.log("onCreateTopic:" + msg.toString());

		// TODO: create the topic in the broker storage
		// the topic is contained in the create topic message

		String topic = msg.getTopic();

		storage.createTopic(topic);

	}

	public void onDeleteTopic(DeleteTopicMsg msg) {

		Logger.log("onDeleteTopic:" + msg.toString());

		// TODO: delete the topic from the broker storage
		// the topic is contained in the delete topic message

		String topic = msg.getTopic();

		storage.deleteTopic(topic);
	}

	public void onSubscribe(SubscribeMsg msg) {

		Logger.log("onSubscribe:" + msg.toString());

		// TODO: subscribe user to the topic
		// user and topic is contained in the subscribe message

		String topic = msg.getTopic();

		storage.addSubscriber(msg.getUser(), topic);

	}

	public void onUnsubscribe(UnsubscribeMsg msg) {

		Logger.log("onUnsubscribe:" + msg.toString());

		// TODO: unsubscribe user to the topic
		// user and topic is contained in the unsubscribe message

		String topic = msg.getTopic();

		storage.removeSubscriber(msg.getUser(), topic);
	}

	public void onPublish(PublishMsg msg) {

		Logger.log("onPublish:" + msg.toString());

		// TODO: publish the message to clients subscribed to the topic
		// topic and message is contained in the subscribe message
		// messages must be sent used the corresponding client session objects

		String topic = msg.getTopic();
		Set<String> recievers = storage.getSubscribers(topic);

		System.out.println("SUBSC:" +recievers.size());

		
		for(String reciever: recievers) {
			ClientSession CS = storage.clients.get(reciever);
			
			if(CS == null) {
				System.out.println("rec  null:" +reciever);
				System.out.println("recievers:" +storage.clients.size());
				if(!storage.bufferedMessages.containsKey(reciever)) {
					storage.bufferedMessages.put(reciever,new ConcurrentLinkedQueue<PublishMsg>() );
				}
				Queue<PublishMsg> messageQueue = storage.bufferedMessages.get(reciever);
				messageQueue.add(msg);
			} else {
				System.out.println("recnot null:" +reciever);
				CS.send(msg);
			}
		}

	}
}

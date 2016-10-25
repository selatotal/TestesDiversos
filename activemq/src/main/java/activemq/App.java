package activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQObjectMessage;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Hello world!
 */
public class App {

	private static String brokerURL = "tcp://0.0.0.0:61613";
	private static String brokerUser = "admin";
	private static String brokerPassword = "password";
	
	public static void main(String[] args) throws Exception {
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		Thread.sleep(1000);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		Thread.sleep(1000);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldProducer(), false);
		Thread.sleep(1000);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldConsumer(), false);
		thread(new HelloWorldProducer(), false);
	}

	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}

	public static class HelloWorldMessage implements Serializable {
		private static final long serialVersionUID = -1922178617939609783L;
		String message;
		Integer id;
		
		HelloWorldMessage(String message, Integer id){
			this.message = message;
			this.id = id;
		}
		
		public String getMessage(){
			return this.message;
		}
		
		public Integer getId(){
			return this.id;
		}
		
		public String toString(){
			return this.message + " - " + this.id;
		}
	}
	
	public static class HelloWorldProducer implements Runnable {
		public void run() {
			try {

				// Create a ConnectionFactory
				ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(App.brokerUser, App.brokerPassword, App.brokerURL);

				// Create a Connection
				Connection connection = connectionFactory.createConnection();
				connection.start();

				// Create a Session
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

				// Create the destination (Topic or Queue)
				Destination destination = session.createQueue("TEST.FOO");

				// Create a MessageProducer from the Session to the Topic or
				// Queue
				MessageProducer producer = session.createProducer(destination);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

				// Create a messages
				String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
				ObjectMessage message = session.createObjectMessage(new HelloWorldMessage(text, this.hashCode()));
				
				// Tell the producer to send the message
				System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
				producer.send(message);

				// Clean up
				session.close();
				connection.close();
			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}
	}

	public static class HelloWorldConsumer implements Runnable, ExceptionListener {
		public void run() {
			try {

				// Create a ConnectionFactory
				ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(App.brokerUser, App.brokerPassword, App.brokerURL);

				// Create a Connection
				Connection connection = connectionFactory.createConnection();
				connection.start();

				connection.setExceptionListener(this);

				// Create a Session
				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

				// Create the destination (Topic or Queue)
				Destination destination = session.createQueue("TEST.FOO");

				// Create a MessageConsumer from the Session to the Topic or
				// Queue
				MessageConsumer consumer = session.createConsumer(destination);

				// Wait for a message
				Message message = consumer.receive(1000);
				if (message instanceof TextMessage) {
					TextMessage textMessage = (TextMessage) message;
					String text = textMessage.getText();
					System.out.println("Received: " + text);
				} if (message instanceof ActiveMQObjectMessage){
					ActiveMQObjectMessage objMessage = (ActiveMQObjectMessage) message;
					HelloWorldMessage helloWorldMessage = (HelloWorldMessage) objMessage.getObject();
					System.out.println("Received Object: " + helloWorldMessage);
				} else {
					System.out.println("Received: " + message);
				}

				consumer.close();
				session.close();
				connection.close();
			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}

		public synchronized void onException(JMSException ex) {
			System.out.println("JMS Exception occured. Shutting down client.");
		}
	}
}
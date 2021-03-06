package topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author Akmzing
 * 点对点消费者1
 */
public class Comsumer2 {
    public static void main(String[] args){
        /** 定义JMS-ActiveMQ连接信息 **/
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Session session = null;
        Destination sendTopic;
        Connection connection = null;
        MessageConsumer consumer = null;
        try {
            /** 进行连接 **/
            connection = connectionFactory.createQueueConnection();
            connection.start();

            /** 建立会话(设置为自动ack) **/
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            /** 建立Topic（当然如果有了就不会重复建立）**/
            sendTopic = session.createTopic("/topic");
            // 建立消息发送者对象
            consumer = session.createConsumer(sendTopic);
            Message message = consumer.receive(1000L);
            while(message==null){
                message = consumer.receive(1000L);
                if(message!=null){
                    TextMessage textMessage = (TextMessage) message;
                    System.out.println("消费者2 Message = " + textMessage.getText());
                }
            }

        }catch (Exception e){
            try {
                session.rollback();
            }catch (Exception e1){
                e1.printStackTrace();
            }
        }finally {
            try {
                /** 关闭 **/
                consumer.close();
                connection.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }

}

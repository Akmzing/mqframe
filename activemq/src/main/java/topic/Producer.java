package topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author Akmzing
 * 点对点生产者
 */
public class Producer {

    public static void main(String[] args) {
        /** 本地tcp协议activemq端口61616 **/
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Session session = null;
        Destination sendTopic = null;
        Connection connection = null;
        MessageProducer sender = null;
        try {
            /**
             * 进行连接
             */
            connection = connectionFactory.createQueueConnection();
            connection.start();

            /** 建立会话（设置一个带有事务特性的会话） **/
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            /** 建立Topic（当然如果有了就不会重复建立）**/
            sendTopic = session.createTopic("/topic");
            /** 建立消息发送者对象 **/
            sender = session.createProducer(sendTopic);
            /**
             * 设置生产者的消息持久化，可以在消费者比生产者后运行也可以收到消息
             * sender.setDeliveryMode(DeliveryMode.PERSISTENT);
             */
            /** 创建消息 **/
            TextMessage outMessage = session.createTextMessage();
            String msg = "这是发送的消息内容Topic";
            System.out.println(msg);

            outMessage.setText(msg);
            /** 发送（JMS是支持事务的）**/
            sender.send(outMessage);

            /**
             * 测试生成效率，目前是5000左右
             long b = System.currentTimeMillis();
             int sum=0;
             while(System.currentTimeMillis()-b<1000){
             outMessage.setText(msg);
             // 发送（JMS是支持事务的
             sender.send(outMessage);
             sum++;
             }
             System.out.println(sum);
             **/
        } catch (Exception exception) {
            try {
                session.rollback();
            } catch (Exception e) {
                e.printStackTrace();
            }
            exception.printStackTrace();
        } finally {
            /** 提交事务、关闭 **/
            try {
                session.commit();
                sender.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}

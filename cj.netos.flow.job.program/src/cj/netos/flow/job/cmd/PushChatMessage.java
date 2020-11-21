package cj.netos.flow.job.cmd;

import cj.netos.flow.job.DefaultBroadcast;
import cj.netos.flow.job.IChatroom;
import cj.netos.flow.job.entities.Chatroom;
import cj.netos.jpush.JPushFrame;
import cj.netos.rabbitmq.CjConsumer;
import cj.netos.rabbitmq.RabbitMQException;
import cj.netos.rabbitmq.RetryCommandException;
import cj.netos.rabbitmq.consumer.IConsumerCommand;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@CjConsumer(name = "chatroom")
@CjService(name = "/chat/message.mq#pushMessage")
public class PushChatMessage extends DefaultBroadcast implements IConsumerCommand {
    @CjServiceRef(refByName = "defaultChatroom")
    IChatroom chatroom;

    @Override
    public void command(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws RabbitMQException, RetryCommandException, IOException {
        Map<String, Object> headers = properties.getHeaders();
        String room = ((LongString) headers.get("room")).toString();
        String msgid = ((LongString) headers.get("msgid")).toString();
        String contentType = ((LongString) headers.get("contentType")).toString();
        String roomcreator = ((LongString) headers.get("creator")).toString();
        String sender = ((LongString) headers.get("sender")).toString();
        String content = new String(body);

        Chatroom chatroom = this.chatroom.getRoom(roomcreator, room);
        if (chatroom == null) {
            CJSystem.logging().warn(getClass(), String.format("聊天室不存在:%s/%s", roomcreator, room));
            return;
        }
        if (chatroom.getFlag()==1) {
            CJSystem.logging().warn(getClass(), String.format("聊天室已删除:%s/%s", roomcreator, room));
            return;
        }
        ByteBuf bb = Unpooled.buffer();
        bb.writeBytes(content.getBytes());
        JPushFrame frame = new JPushFrame("pushMessage /chat/room/message gbera/1.0", bb);
        frame.parameter("room", room);
        frame.parameter("roomCreator", roomcreator);
        frame.parameter("contentType", contentType);
        frame.parameter("msgid", msgid);
        frame.parameter("ctime", System.currentTimeMillis() + "");
        frame.head("sender-person", sender);

        List<String> sendedPersons = new ArrayList<>();

        long limit = 100;
        long skip = 0;
        while (true) {
            List<String> members = this.chatroom.pageMember(chatroom.getCreator(), room, limit, skip);
            if (members.isEmpty()) {
                break;
            }
            skip += members.size();
            for (String person : members) {
                if (sendedPersons.contains(person)) {
                    continue;
                }
                frame.head("to-person", person);
                try {
                    broadcast(frame.copy());
                } catch (CircuitException e) {
                    throw new RabbitMQException(e);
                }
                sendedPersons.add(person);
            }
        }
    }
}

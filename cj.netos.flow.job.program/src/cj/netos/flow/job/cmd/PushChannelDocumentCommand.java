package cj.netos.flow.job.cmd;

import cj.netos.flow.job.DefaultBroadcast;
import cj.netos.flow.job.IChannel;
import cj.netos.flow.job.entities.ChannelDocument;
import cj.netos.jpush.JPushFrame;
import cj.netos.rabbitmq.CjConsumer;
import cj.netos.rabbitmq.RabbitMQException;
import cj.netos.rabbitmq.RetryCommandException;
import cj.netos.rabbitmq.consumer.IConsumerCommand;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import cj.ultimate.gson2.com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@CjConsumer(name = "channel")
@CjService(name = "/channel/document.mq#pushChannelDocument")
public class PushChannelDocumentCommand extends DefaultBroadcast implements IConsumerCommand {
    @CjServiceRef(refByName = "defaultChannel")
    IChannel channel;


    @Override
    public void command(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws RabbitMQException, RetryCommandException, IOException {
        Map<String, Object> headers = properties.getHeaders();
        String creator = ((LongString) headers.get("creator")).toString();
        String channel = ((LongString) headers.get("channel")).toString();
        String docid = ((LongString) headers.get("docid")).toString();
        String sender = ((LongString) headers.get("sender")).toString();

        ChannelDocument doc = this.channel.getDocument(creator, channel, docid);
        if (doc == null) {
            CJSystem.logging().warn(getClass(), String.format("文档不存在:%s/%s", channel, docid));
            return;
        }
        ByteBuf bb = Unpooled.buffer();
        bb.writeBytes(new Gson().toJson(doc).getBytes());
        JPushFrame frame = new JPushFrame("pushDocument /netflow/channel gbera/1.0", bb);
        frame.parameter("docid", docid);
        frame.parameter("channel", channel);
        frame.parameter("creator", creator);
        frame.head("sender-person", sender);

        List<String> sendedPerson = new ArrayList<>();
        //先推送给创建者
        frame.head("to-person", creator);
        try {
            broadcast(frame.copy());
        } catch (CircuitException e) {
            throw new RabbitMQException(e);
        }
        sendedPerson.add(creator);

        long limit = 100;
        long skip = 0;
        while (true) {
            List<String> outputPersons = this.channel.findOutputPersons(sender, channel, limit, skip);
            if (outputPersons.isEmpty()) {
                break;
            }
            skip += outputPersons.size();
            for (String person : outputPersons) {
                if (sendedPerson.contains(person)) {
                    continue;
                }
                frame.head("to-person", person);
                try {
                    broadcast(frame.copy());
                } catch (CircuitException e) {
                    throw new RabbitMQException(e);
                }
                sendedPerson.add(person);
            }
        }
        frame.dispose();
        sendedPerson.clear();
    }
}

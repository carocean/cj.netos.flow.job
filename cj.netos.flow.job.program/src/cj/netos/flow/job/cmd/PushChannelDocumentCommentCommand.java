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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CjConsumer(name = "channel")
@CjService(name = "/channel/document/comment.mq#pushChannelDocumentComment")
public class PushChannelDocumentCommentCommand extends DefaultBroadcast implements IConsumerCommand {
    @CjServiceRef(refByName = "defaultChannel")
    IChannel channel;

    @Override
    public void command(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws RabbitMQException, RetryCommandException, IOException {
        Map<String, Object> headers = properties.getHeaders();
        String creator = ((LongString) headers.get("creator")).toString();
        String channel = ((LongString) headers.get("channel")).toString();
        String docid = ((LongString) headers.get("docid")).toString();
        String commenter = ((LongString) headers.get("commenter")).toString();
        String commentid = ((LongString) headers.get("commentid")).toString();
        String comments = new String(body);

        ChannelDocument doc = this.channel.getDocument(creator, channel, docid);
        if (doc == null) {
            CJSystem.logging().warn(getClass(), String.format("文档不存在:%s/%s", channel, docid));
            return;
        }
        ByteBuf bb = Unpooled.buffer();
        Map<String, Object> map = new HashMap<>();
        map.put("doc", doc);
        map.put("comments", comments);

        bb.writeBytes(new Gson().toJson(map).getBytes());
        JPushFrame frame = new JPushFrame("commentDocument /netflow/channel gbera/1.0", bb);
        frame.parameter("commenter", commenter);
        frame.parameter("docid", docid);
        frame.parameter("channel", channel);
        frame.parameter("creator", creator);
        frame.parameter("commentid", commentid);

        List<String> sendedPerson = new ArrayList<>();
        //先推送给创建者
        frame.head("sender-person", commenter);
        frame.head("to-person", creator);
        try {
            broadcast(frame.copy());
        } catch (CircuitException e) {
            throw new RabbitMQException(e);
        }
        sendedPerson.add(creator);

        long limit = 100;
        long skip = 0;
        CJSystem.logging().debug(getClass(), String.format("开始推送输出公众"));
        while (true) {
            List<String> outputPersons = this.channel.findOutputPersons(commenter, channel, limit, skip);
            if (outputPersons.isEmpty()) {
                break;
            }
            skip += outputPersons.size();
            frame.head("sender-person", commenter);
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
        CJSystem.logging().debug(getClass(), String.format("开始推送流转用户"));
        skip = 0;
        while (true) {
            List<String> activities = this.channel.findFlowActivities(creator, docid, channel, limit, skip);
            if (activities.isEmpty()) {
                break;
            }
            skip += activities.size();
            frame.head("sender-person", commenter);
            for (String person : activities) {
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

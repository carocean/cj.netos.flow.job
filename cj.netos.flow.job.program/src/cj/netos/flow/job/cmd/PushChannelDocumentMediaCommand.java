package cj.netos.flow.job.cmd;

import cj.netos.flow.job.DefaultBroadcast;
import cj.netos.flow.job.IChannel;
import cj.netos.jpush.JPushFrame;
import cj.netos.rabbitmq.CjConsumer;
import cj.netos.rabbitmq.RabbitMQException;
import cj.netos.rabbitmq.RetryCommandException;
import cj.netos.rabbitmq.consumer.IConsumerCommand;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CjConsumer(name = "channel")
@CjService(name = "/channel/document/media.mq#pushChannelDocumentMedia")
public class PushChannelDocumentMediaCommand extends DefaultBroadcast implements IConsumerCommand {
    @CjServiceRef(refByName = "defaultChannel")
    IChannel channel;

    @Override
    public void command(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws RabbitMQException, RetryCommandException, IOException {
        Map<String, Object> headers = properties.getHeaders();
        String creator = ((LongString) headers.get("creator")).toString();
        Map media = new Gson().fromJson(new String(body), HashMap.class);

        ByteBuf bb = Unpooled.buffer();
        bb.writeBytes(new Gson().toJson(media).getBytes());
        JPushFrame frame = new JPushFrame("mediaDocument /netflow/channel gbera/1.0", bb);
        frame.parameter("creator", creator);
        long limit = 100;
        long skip = 0;
        while (true) {
            List<String> outputPersons = this.channel.findOutputPersons(creator, (String) media.get("channel"), limit, skip);
            if (outputPersons.isEmpty()) {
                break;
            }
            skip += outputPersons.size();
            frame.head("sender-person", creator);
            for (String person : outputPersons) {
                frame.head("to-person", person);
                try {
                    broadcast(frame.copy());
                } catch (CircuitException e) {
                    throw new RabbitMQException(e);
                }
            }
        }
        frame.dispose();
    }
}

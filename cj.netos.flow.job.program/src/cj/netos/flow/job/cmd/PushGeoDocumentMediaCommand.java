package cj.netos.flow.job.cmd;

import cj.netos.flow.job.entities.GeoDocument;
import cj.netos.jpush.JPushFrame;
import cj.netos.rabbitmq.CjConsumer;
import cj.netos.rabbitmq.RabbitMQException;
import cj.netos.rabbitmq.RetryCommandException;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.annotation.CjService;
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

@CjConsumer(name = "geosphere")
@CjService(name = "/geosphere/document/media.mq#pushGeoDocumentMedia")
public class PushGeoDocumentMediaCommand extends PushGeoFlowJobBase {
    @Override
    public void command(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws RabbitMQException, RetryCommandException, IOException {
        Map<String, Object> headers = properties.getHeaders();
        String mediacreator = ((LongString) headers.get("creator")).toString();
        Map<String, Object> media = new Gson().fromJson(new String(body), HashMap.class);
        String docid = (String) media.get("docid");

        GeoDocument doc = this.receptor.getDocument(docid);
        if (doc == null) {
            CJSystem.logging().warn(getClass(), String.format("文档不存在:%s", docid));
            return;
        }
        ByteBuf bb = Unpooled.buffer();
        bb.writeBytes(new Gson().toJson(media).getBytes());
        JPushFrame frame = new JPushFrame("mediaDocument /geosphere/receptor gbera/1.0", bb);
        String creator = doc.getCreator();
        frame.parameter("docid", docid);
        frame.parameter("creator", creator);
        frame.head("sender-person", mediacreator);

        Map<String, List<String>> destinations = getDestinations( doc.getReceptor(), creator);
//        CJSystem.logging().warn(getClass(), String.format("推送目标:%s", new Gson().toJson(destinations)));
        try {
            broadcast(destinations, frame);
        } catch (CircuitException e) {
            throw new RabbitMQException(e);
        }

        frame.dispose();
        destinations.clear();
    }
}

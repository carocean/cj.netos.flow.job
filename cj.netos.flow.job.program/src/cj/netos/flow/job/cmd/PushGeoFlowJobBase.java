package cj.netos.flow.job.cmd;

import cj.netos.flow.job.DefaultBroadcast;
import cj.netos.flow.job.IGeoReceptor;
import cj.netos.jpush.JPushFrame;
import cj.netos.rabbitmq.consumer.IConsumerCommand;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import cj.ultimate.gson2.com.google.gson.Gson;

import java.util.*;

public abstract class PushGeoFlowJobBase extends DefaultBroadcast implements IConsumerCommand {
    @CjServiceRef(refByName = "defaultReceptor")
    IGeoReceptor receptor;

    protected void broadcast(Map<String, List<String>> destinations, JPushFrame frame) throws CircuitException {
        Set<String> persons = destinations.keySet();
        for (String person : persons) {
            List<String> ids = destinations.get(person);
            frame.head("to-receptors", new Gson().toJson(ids));
            frame.head("to-person", person);
            broadcast(frame.copy());
        }
    }

    protected Map<String, List<String>> getDestinations(String receptor, String creator) {
        Map<String, List<String>> destinations = new HashMap<>();//key是person
        //消息创建者发消息创建者的目标注释掉
//        keypair.add(String.format("%s/%s", category, receptor));
//        destinations.put(creator, keypair);

        long limit = 100;
        long skip = 0;
        while (true) {
            List<String> followers = this.receptor.pageReceptorFans( receptor, limit, skip);
            if (followers.isEmpty()) {
                break;
            }
            CJSystem.logging().warn(getClass(), String.format("粉丝数:%s", followers.size()));
            skip += followers.size();
            for (String person : followers) {
                List<String>  keypair = destinations.get(person);
                if (keypair == null) {
                    keypair = new ArrayList<>();
                    destinations.put(person, keypair);
                }
                if (keypair.contains(receptor)) {
                    continue;
                }
                keypair.add(receptor);
            }
        }
        skip = 0;
        while (true) {
            Map<String, List<String>> personReceptors = this.receptor.searchAroundReceptors(receptor, "mobiles", limit, skip);
            if (personReceptors.isEmpty()) {
                break;
            }
            CJSystem.logging().warn(getClass(), String.format("感知用户数:%s", personReceptors.size()));
            skip += personReceptors.size();
            Set<String> creators = personReceptors.keySet();
            for (String person : creators) {
                if (destinations.containsKey(person)) {//如果粉丝感知器已有则排除再向其行人感知器推送
                    continue;
                }
                List<String>  keypair = destinations.get(person);
                if (keypair == null) {
                    keypair = new ArrayList<>();
                    destinations.put(person, keypair);
                }
                List<String> _keypairs = personReceptors.get(person);
                for (String id : _keypairs) {
                    if (keypair.contains(id)) {
                        continue;
                    }
                    keypair.add(id);
                }
            }
        }

        return destinations;
    }
}

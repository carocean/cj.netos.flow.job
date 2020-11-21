package cj.netos.flow.job.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.netos.flow.job.AbstractService;
import cj.netos.flow.job.IChatroom;
import cj.netos.flow.job.entities.Chatroom;
import cj.studio.ecm.annotation.CjService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CjService(name = "defaultChatroom")
public class DefaultChatroom extends AbstractService implements IChatroom {

    @Override
    public Chatroom getRoom(String msgcreator, String room) {
        ICube cube = cube(msgcreator);
        String cjql = String.format("select {'tuple':'*'}.limit(1) from tuple chat.rooms %s where {'tuple.room':'%s'}", Chatroom.class.getName(), room);
        IQuery<Chatroom> query = cube.createQuery(cjql);
        IDocument<Chatroom> doc = query.getSingleResult();
        if (doc == null) {
            return null;
        }
        return doc.tuple();
    }

    @Override
    public List<String> pageMember(String msgcreator, String room, long limit, long skip) {
        ICube cube = cube(msgcreator);
        String cjql = String.format("select {'tuple.person':1}.limit(%s).skip(%s) from tuple chat.members %s where {'tuple.room':'%s','tuple.flag':{'$ne':1}}  ", limit, skip, HashMap.class.getName(), room);
        IQuery<Map<String, String>> query = cube.createQuery(cjql);
        List<IDocument<Map<String, String>>> docs = query.getResultList();
        List<String> members = new ArrayList<>();
        for (IDocument<Map<String, String>> doc : docs) {
            members.add(doc.tuple().get("person"));
        }
        return members;
    }
}

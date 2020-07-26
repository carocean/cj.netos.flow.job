package cj.netos.flow.job;


import cj.netos.flow.job.entities.ChannelDocument;
import cj.netos.flow.job.entities.ChannelDocumentComment;

import java.util.List;

public interface IChannel {
    List<String> findOutputPersons(String person, String channel, long limit, long skip);

    ChannelDocument getDocument(String person, String channel, String docid);

    ChannelDocumentComment getDocumentComment(String creator, String channel, String docid, String commentid);

    List<String> findFlowActivities(String creator, String docid, String channel, long limit, long skip);

}

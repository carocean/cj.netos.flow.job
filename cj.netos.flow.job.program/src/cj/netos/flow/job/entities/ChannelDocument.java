package cj.netos.flow.job.entities;

import java.util.List;

public class ChannelDocument {
    String id;
    String creator;
    String channel;
    double wy;
    long ctime;
    String content;

    private List<ChannelDocumentMedia> medias;

    public void setMedias(List<ChannelDocumentMedia> medias) {
        this.medias = medias;
    }

    public List<ChannelDocumentMedia> getMedias() {
        return medias;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public double getWy() {
        return wy;
    }

    public void setWy(double wy) {
        this.wy = wy;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}

package cj.netos.flow.job.entities;

import cj.ultimate.gson2.com.google.gson.Gson;

import java.util.List;
import java.util.Map;

public class GeoDocument {

    private List<GeoDocumentMedia> medias;

    public static GeoDocument parse(Map<String, Object> tuple) {
        String json = new Gson().toJson(tuple);
        return new Gson().fromJson(json,GeoDocument.class);
    }
    String id;
    String upstreamPerson;
    //如果是从网流来的消息
    String upstreamChannel;
    String sourceSite;
    String sourceApp;
    String receptor;
    String creator;
    long ctime;
    long atime;
    long rtime;
    long dtime;
    String state;
    String text;
    double wy;
    ///location是GEOPoi对象
    LatLng location;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUpstreamPerson() {
        return upstreamPerson;
    }

    public void setUpstreamPerson(String upstreamPerson) {
        this.upstreamPerson = upstreamPerson;
    }

    public String getUpstreamChannel() {
        return upstreamChannel;
    }

    public void setUpstreamChannel(String upstreamChannel) {
        this.upstreamChannel = upstreamChannel;
    }

    public String getSourceSite() {
        return sourceSite;
    }

    public void setSourceSite(String sourceSite) {
        this.sourceSite = sourceSite;
    }

    public String getSourceApp() {
        return sourceApp;
    }

    public void setSourceApp(String sourceApp) {
        this.sourceApp = sourceApp;
    }

    public String getReceptor() {
        return receptor;
    }

    public void setReceptor(String receptor) {
        this.receptor = receptor;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public long getAtime() {
        return atime;
    }

    public void setAtime(long atime) {
        this.atime = atime;
    }

    public long getRtime() {
        return rtime;
    }

    public void setRtime(long rtime) {
        this.rtime = rtime;
    }

    public long getDtime() {
        return dtime;
    }

    public void setDtime(long dtime) {
        this.dtime = dtime;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getWy() {
        return wy;
    }

    public void setWy(double wy) {
        this.wy = wy;
    }

    public LatLng getLocation() {
        return location;
    }

    public void setLocation(LatLng location) {
        this.location = location;
    }

    public void setMedias(List<GeoDocumentMedia> medias) {
        this.medias=medias;
    }
}

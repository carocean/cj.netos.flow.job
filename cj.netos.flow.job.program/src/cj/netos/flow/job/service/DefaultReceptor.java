package cj.netos.flow.job.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.netos.flow.job.IGeoReceptor;
import cj.netos.flow.job.entities.*;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.ultimate.gson2.com.google.gson.Gson;
import cj.ultimate.util.StringUtil;
import com.mongodb.client.AggregateIterable;
import org.bson.Document;

import java.util.*;

@CjService(name = "defaultReceptor")
public class DefaultReceptor implements IGeoReceptor {
    final static String _KEY_CATEGORY_COL = "geo.categories";
    @CjServiceRef(refByName = "mongodb.netos.home")
    ICube home;

    String _getReceptorColName() {
        return String.format("geo.receptors");
    }

    String _getDocumentColName() {
        return String.format("geo.receptor.docs");
    }

    String _getMediaColName() {
        return String.format("geo.receptor.medias");
    }

    String _getFollowColName() {
        return String.format("geo.receptor.follows");
    }

    @Override
    public GeoDocument getDocument( String docid) {
        String cjql = String.format("select {'tuple':'*'}.limit(1) from tuple ?(colname) ?(clazz) where {'tuple.id':'?(docid)'}");
        IQuery<GeoDocument> query = home.createQuery(cjql);
        query.setParameter("colname", _getDocumentColName());
        query.setParameter("clazz", GeoDocument.class.getName());
        query.setParameter("docid", docid);
        IDocument<GeoDocument> doc = query.getSingleResult();
        if (doc == null) {
            return null;
        }
        cjql = String.format("select {'tuple':'*'}.limit(1) from tuple ?(colname) ?(clazz) where {'tuple.docid':'?(docid)'}");
        IQuery<GeoDocumentMedia> query2 = home.createQuery(cjql);
        query2.setParameter("colname", _getMediaColName());
        query2.setParameter("clazz", GeoDocumentMedia.class.getName());
        query2.setParameter("docid", docid);
        List<IDocument<GeoDocumentMedia>> medias = query2.getResultList();
        List<GeoDocumentMedia> _medias = new ArrayList<>();
        for (IDocument<GeoDocumentMedia> media : medias) {
            _medias.add(media.tuple());
        }
        GeoDocument geoDocument = doc.tuple();
        geoDocument.setMedias(_medias);
        return geoDocument;
    }

    public GeoReceptor getReceptor( String receptor) {
        String cjql = "select {'tuple':'*'} from tuple ?(colname) ?(clazz) where {'tuple.id':'?(receptor)'}";
        IQuery<GeoReceptor> query = home.createQuery(cjql);
        query.setParameter("colname", _getReceptorColName());
        query.setParameter("clazz", GeoReceptor.class.getName());
        query.setParameter("receptor", receptor);
        IDocument<GeoReceptor> doc = query.getSingleResult();
        if (doc == null) {
            return null;
        }
        return doc.tuple();
    }

    @Override
    public Map<String, List<String>> searchAroundReceptors( String receptor, String geoType, long limit, long skip) {
        GeoReceptor geoReceptor = getReceptor(receptor);
        List<String> categoryIds = new ArrayList<>();
        if (!StringUtil.isEmpty(geoType)) {
            String[] arr = geoType.split("\\|");
            for (String t : arr) {
                categoryIds.add(t);
            }
        }
        LatLng latLng = geoReceptor.getLocation();
        double radius = geoReceptor.getRadius();
        Map<String, List<String>> receptorsOfCreatorMap = new HashMap<>();
       _searchPoiInCategory(latLng, radius,categoryIds, limit, skip,receptorsOfCreatorMap);
        return receptorsOfCreatorMap;
    }

    private void _searchPoiInCategory(LatLng location, double radius,List<String> categoryIds,long limit, long skip, Map<String, List<String>> receptorsOfCreatorMap) {
        //distanceField:"distance" 距离字段别称
        //"distanceMultiplier": 0.001,
        //{ $limit : 5 }
        String json = String.format("{" +
                "'$geoNear':{" +
                "'near':{'type':'Point','coordinates':%s}," +
                "'distanceField':'tuple.distance'," +
                "'maxDistance':%s," +
                "'spherical':true" +
                "}" +
                "}", location.toCoordinate(), radius);

        String limitjson = String.format("{'$limit':%s}", limit);
        String skipjson = String.format("{'$skip':%s}", skip);
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(Document.parse(json));
        if (!categoryIds.isEmpty()) {
            String matchjson = String.format("{'$match':{'tuple.category':{'$in':%s}}}", new Gson().toJson(categoryIds));
            pipeline.add(Document.parse(matchjson));
        }
        pipeline.add(Document.parse(limitjson));
        pipeline.add(Document.parse(skipjson));
        AggregateIterable<Document> it = home.aggregate(_getReceptorColName(), pipeline);
        for (Document doc : it) {
            Map<String, Object> tuple = (Map<String, Object>) doc.get("tuple");
            String creator = (String) tuple.get("creator");
            String id = (String) tuple.get("id");
//            String cate=(String)tuple.get("category");
            List<String> receptors = receptorsOfCreatorMap.get(creator);
            if (receptors == null) {
                receptors = new ArrayList<>();
                receptorsOfCreatorMap.put(creator, receptors);
            }
            receptors.add(id);
        }
    }

    @Override
    public List<String> pageReceptorFans(String receptor, long limit, long skip) {
        GeoReceptor geoReceptor = getReceptor(receptor);

        String cjql = "select {'tuple':'*'}.limit(?(limit)).skip(?(skip)) from tuple ?(colname) ?(clazz) where {'tuple.receptor':'?(receptor)'}";
        IQuery<GeoFollow> query = home.createQuery(cjql);
        query.setParameter("colname", _getFollowColName());
        query.setParameter("clazz", GeoFollow.class.getName());
        query.setParameter("receptor", geoReceptor.getId());
        query.setParameter("limit", limit+"");
        query.setParameter("skip",skip+"");
        List<IDocument<GeoFollow>> docs = query.getResultList();
        List<String> ids = new ArrayList<>();
//        Map<String, GeoFollow> follows = new HashMap<>();
        for (IDocument<GeoFollow> doc : docs) {
            String follower=doc.tuple().getPerson();
            ids.add(follower);
//            follows.put(doc.tuple().getPerson(), doc.tuple());
        }
        return ids;
    }
}

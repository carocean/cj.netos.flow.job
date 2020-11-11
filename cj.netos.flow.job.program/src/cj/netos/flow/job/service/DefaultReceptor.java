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

    public List<GeoCategory> listCategory() {
        String cjql = "select {'tuple':'*'} from tuple ?(colname) ?(clazz) where {}";
        IQuery<GeoCategory> query = home.createQuery(cjql);
        query.setParameter("colname", _KEY_CATEGORY_COL);
        query.setParameter("clazz", GeoCategory.class.getName());
        List<IDocument<GeoCategory>> list = query.getResultList();
        List<GeoCategory> categories = new ArrayList<>();
        for (IDocument<GeoCategory> doc : list) {
            categories.add(doc.tuple());
        }
        return categories;
    }

    public List<GeoCategory> findCategories(String geoType) {
        String[] arr = geoType.split("\\|");
        List<String> ids = new ArrayList();
        for (String id : arr) {
            if (StringUtil.isEmpty(id)) {
                continue;
            }
            ids.add(id);
        }
        String cjql = "select {'tuple':'*'} from tuple ?(colname) ?(clazz) where {'tuple.id':{'$in':?(ids)}}";
        IQuery<GeoCategory> query = home.createQuery(cjql);
        query.setParameter("colname", _KEY_CATEGORY_COL);
        query.setParameter("clazz", GeoCategory.class.getName());
        query.setParameter("ids", new Gson().toJson(ids));
        List<IDocument<GeoCategory>> list = query.getResultList();
        List<GeoCategory> categories = new ArrayList<>();
        for (IDocument<GeoCategory> doc : list) {
            categories.add(doc.tuple());
        }
        return categories;
    }

    @Override
    public Map<String, List<String>> searchAroundReceptors( String receptor, String geoType, long limit, long skip) {
        List<GeoCategory> categories = null;
        if (StringUtil.isEmpty(geoType)) {
            categories = listCategory();
        } else {
            categories = findCategories(geoType);
        }
        GeoReceptor geoReceptor = getReceptor(receptor);
        LatLng latLng = geoReceptor.getLocation();
        double radius = geoReceptor.getRadius();
        Map<String, List<String>> receptorsOfCreatorMap = new HashMap<>();
        for (GeoCategory cate : categories) {
            _searchPoiInCategory(latLng, radius, limit, skip, receptorsOfCreatorMap);
        }
        return receptorsOfCreatorMap;
    }

    private void _searchPoiInCategory(LatLng location, double radius,long limit, long skip, Map<String, List<String>> receptorsOfCreatorMap) {
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
        AggregateIterable<Document> it = home.aggregate(_getReceptorColName(), Arrays.asList(Document.parse(json), Document.parse(limitjson), Document.parse(skipjson)));
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

        String cjql = "select {'tuple':'*'}.limit(?(limit)).skip(?(skip)) from tuple ?(colname) ?(clazz) where {'tuple.category':'?(category)','tuple.receptor':'?(receptor)'}";
        IQuery<GeoFollow> query = home.createQuery(cjql);
        query.setParameter("colname", _getFollowColName());
        query.setParameter("clazz", GeoFollow.class.getName());
        query.setParameter("category", geoReceptor.getCategory());
        query.setParameter("receptor", geoReceptor.getId());
        query.setParameter("limit", limit+"");
        query.setParameter("skip",skip+"");
        List<IDocument<GeoFollow>> docs = query.getResultList();
        List<String> ids = new ArrayList<>();
        Map<String, GeoFollow> follows = new HashMap<>();
        for (IDocument<GeoFollow> doc : docs) {
            ids.add(doc.tuple().getPerson());
            follows.put(doc.tuple().getPerson(), doc.tuple());
        }

        LatLng latLng = geoReceptor.getLocation();
        //由于粉丝不在感知器半径内，也能收到消息，所以设为最大公里数求粉丝
        double radius = /*geoReceptor.getRadius()*/Long.MAX_VALUE;
        //查询这些用户的mobiles分类下的感知器，并以此作为远近距离排序
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
                "}", latLng.toCoordinate(), radius);
        String match = String.format("{'$match':{'tuple.creator':{'$in':%s}}}", new Gson().toJson(ids));
        AggregateIterable<Document> it = home.aggregate(_getReceptorColName(), Arrays.asList(Document.parse(json), Document.parse(match)));
        ids.clear();
        for (Document doc : it) {
            Map<String, Object> tuple = (Map<String, Object>) doc.get("tuple");
//            double distance = (double) tuple.get("distance");
            String creator = (String) tuple.get("creator");
            if (ids.contains(creator)) {
                //注意，如果一个人有多个mobiles感知器会造成重复记录，因此排除掉
                continue;
            }
            ids.add(creator);
        }
        return ids;
    }
}

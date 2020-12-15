package cj.netos.flow.job;


import cj.netos.flow.job.entities.GeoDocument;
import cj.netos.flow.job.entities.GeoReceptor;

import java.util.List;
import java.util.Map;

public interface IGeoReceptor {
    GeoDocument getDocument(String docid);
    Map<String, List<String>> searchAroundReceptors( String receptor, String geoType, long limit, long skip);
    List<String> pageReceptorFans(  String receptor, long limit, long skip);

    GeoReceptor getReceptor(String receptor);

}

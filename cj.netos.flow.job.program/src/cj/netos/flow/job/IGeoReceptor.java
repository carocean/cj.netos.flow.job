package cj.netos.flow.job;


import cj.netos.flow.job.entities.GeoDocument;

import java.util.List;
import java.util.Map;

public interface IGeoReceptor {
    GeoDocument getDocument(String category, String receptor, String docid);
    Map<String, List<String>> searchAroundReceptors(String category, String receptor, String geoType, long limit, long skip);
    List<String> pageReceptorFans( String category, String receptor, long limit, long skip);
}

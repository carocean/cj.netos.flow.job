package cj.netos.flow.job.program;

import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.net.Circuit;
import cj.studio.ecm.net.CircuitException;
import cj.studio.ecm.net.Frame;
import cj.studio.gateway.socket.app.IGatewayAppSiteResource;
import cj.studio.gateway.socket.app.IGatewayAppSiteWayWebView;
import cj.ultimate.gson2.com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * @author caroceanjofers
 */
@CjService(name = "/")
public class Home implements IGatewayAppSiteWayWebView {

    @Override
    public void flow(Frame frame, Circuit circuit, IGatewayAppSiteResource resource) throws CircuitException {
        Map<String, Object> map = new HashMap<>();
        map.put("status", "801");
        map.put("message", "拒绝访问");
        circuit.content().writeBytes(new Gson().toJson(map).getBytes());
    }

}

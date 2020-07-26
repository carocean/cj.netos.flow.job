package cj.netos.flow.job;

import cj.netos.jpush.JPushFrame;
import cj.studio.ecm.net.CircuitException;

public interface IBroadcast {
    void broadcast(JPushFrame frame) throws CircuitException;
}

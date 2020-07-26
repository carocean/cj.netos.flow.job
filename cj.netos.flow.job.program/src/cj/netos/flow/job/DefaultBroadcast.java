package cj.netos.flow.job;

import cj.netos.jpush.JPushFrame;
import cj.netos.jpush.pusher.IJPusher;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;

public abstract class DefaultBroadcast implements IBroadcast {
    @CjServiceRef(refByName = "@.jpusher")
    IJPusher pusher;
    @Override
    public void broadcast(JPushFrame frame) throws CircuitException {
        pusher.push(frame.copy());
    }
}

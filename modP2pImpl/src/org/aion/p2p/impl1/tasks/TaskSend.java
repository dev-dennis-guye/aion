package org.aion.p2p.impl1.tasks;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.aion.p2p.INode;
import org.aion.p2p.INodeMgr;
import org.aion.p2p.IP2pMgr;
import org.aion.p2p.P2pConstant;
import org.slf4j.Logger;

public class TaskSend implements Runnable {

    private final Logger p2pLOG;
    private final IP2pMgr mgr;
    private final AtomicBoolean start;
    private final BlockingQueue<MsgOut> sendMsgQue;
    private final INodeMgr nodeMgr;
    private final Selector selector;
    private final int lane;
    private final ThreadPoolExecutor tpe;
    private static final int THREAD_Q_LIMIT = 20000;

    public TaskSend(
            final Logger p2pLOG,
            final IP2pMgr _mgr,
            final int _lane,
            final BlockingQueue<MsgOut> _sendMsgQue,
            final AtomicBoolean _start,
            final INodeMgr _nodeMgr,
            final Selector _selector) {

        this.p2pLOG = p2pLOG;
        this.mgr = _mgr;
        this.lane = _lane;
        this.sendMsgQue = _sendMsgQue;
        this.start = _start;
        this.nodeMgr = _nodeMgr;
        this.selector = _selector;
        this.tpe =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(THREAD_Q_LIMIT),
                        Executors.defaultThreadFactory());
    }

    @Override
    public void run() {
        while (start.get()) {
            try {
                MsgOut mo = sendMsgQue.take();

                // if timeout , throw away this msg.
                long now = System.currentTimeMillis();
                if (now - mo.getTimestamp() > P2pConstant.WRITE_MSG_TIMEOUT) {
                    if (p2pLOG.isDebugEnabled()) {
                        p2pLOG.debug("timeout-msg to-node={} timestamp={}", mo.getDisplayId(), now);
                    }
                    continue;
                }

                // if not belong to current lane, put it back.
                if (mo.getLane() != lane) {
                    sendMsgQue.offer(mo);
                    continue;
                }

                INode node = null;
                switch (mo.getDest()) {
                    case ACTIVE:
                        node = nodeMgr.getActiveNode(mo.getNodeId());
                        break;
                    case INBOUND:
                        node = nodeMgr.getInboundNode(mo.getNodeId());
                        break;
                    case OUTBOUND:
                        node = nodeMgr.getOutboundNode(mo.getNodeId());
                        break;
                }

                if (node != null) {
                    SelectionKey sk = node.getChannel().keyFor(selector);
                    if (sk != null) {
                        Object attachment = sk.attachment();
                        if (attachment != null) {
                            tpe.execute(
                                    new TaskWrite(
                                            p2pLOG,
                                            node.getIdShort(),
                                            node.getChannel(),
                                            mo.getMsg(),
                                            (ChannelBuffer) attachment,
                                            this.mgr));
                        }
                    }
                } else {
                    if (p2pLOG.isDebugEnabled()) {
                        p2pLOG.debug(
                                "msg-{} ->{} node-not-exist",
                                mo.getDest().name(),
                                mo.getDisplayId());
                    }
                }
            } catch (InterruptedException e) {
                p2pLOG.error("task-send-interrupted", e);
                return;
            } catch (RejectedExecutionException e) {
                p2pLOG.warn("task-send-reached thread queue limit", e);
            } catch (Exception e) {
                if (p2pLOG.isDebugEnabled()) {
                    p2pLOG.debug("TaskSend exception.", e);
                }
            }
        }
    }

    // hash mapping channel id to write thread.
    static int hash2Lane(int in) {
        in ^= in >> (32 - 5);
        in ^= in >> (32 - 10);
        in ^= in >> (32 - 15);
        in ^= in >> (32 - 20);
        in ^= in >> (32 - 25);
        return (in & 0b11111);
    }
}

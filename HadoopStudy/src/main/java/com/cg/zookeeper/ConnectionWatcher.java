package com.cg.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public class ConnectionWatcher implements Watcher {

	private static final int SESSION_TIMEOUT=5000;
	
	protected ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public ZooKeeper getZooKeeper(){
		return zk;
	}
	
	protected void connect(String host) throws IOException, InterruptedException{
		zk= new ZooKeeper(host, SESSION_TIMEOUT, this);
		connectedSignal.await();
	} 
	
	@Override
	public void process(WatchedEvent event) {
		if (event.getState()==KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
		if (event.getType()==EventType.NodeDataChanged) {
			System.out.println("���ݸı�");
		}
	}

	public void close() throws InterruptedException{
		zk.close();
	}
}

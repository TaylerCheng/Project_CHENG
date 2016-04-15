package com.cg.zookeeper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ZookeeperUtil {

	private static final int SESSION_TIMEOUT = 5000;
	private static final Charset CHARSET = Charset.forName("UTF-8");

	private static ConnectionWatcher cw = new ConnectionWatcher();
	private static ZooKeeper zk;

	private ZookeeperUtil() {
	}

	private static void connect(String host) {
		try {
			cw.connect(host);
			zk = cw.getZooKeeper();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void createNode(String groupName, String memberName)
			throws KeeperException, InterruptedException {
		if (groupName.equals("/")) {
			groupName = "";
		}
		String path = groupName + "/" + memberName;
		zk.create(path, "12345".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	private static void list(String groupName) {
		try {
			List<String> children = zk.getChildren(groupName, false);
			System.out.println("---- " + groupName + " has " + children.size()
					+ " children ----");
			if (children.isEmpty()) {
				System.out.println("is empty");
			}
			for (String child : children) {
				System.out.print(child + ": ");
				if (groupName.equals("/")) {
					groupName = "";
				}
				byte[] data = zk.getData(groupName + "/" + child, cw, null);
				System.out.println(new String(data, CHARSET));
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void delete(String memberName) throws InterruptedException,
			KeeperException {
		zk.delete(memberName, -1);
	}

	private static void setData(String path, String value)
			throws KeeperException, InterruptedException {
		byte[] data = value.getBytes();
		zk.setData(path, data, -1);
	}

	private static String getData(String memberName) throws KeeperException,
			InterruptedException {
		byte[] data = zk.getData(memberName, cw, null);
		System.out.println(new String(data, CHARSET));
		return data.toString();
	}

	private static void close() throws InterruptedException {
		zk.close();
	}

	private static void createNode(String path) throws KeeperException,
			InterruptedException {
		zk.create(path, "12345".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
	}

	private static void getACL(String path) throws KeeperException,
			InterruptedException {
		System.out.println(zk.getACL(path, new Stat()));
	}

	public static void main(String[] args) throws Exception {

		ZookeeperUtil.connect("172.16.5.102:3181");
		// ZookeeperUtil.createNode("/cheng","xcg_");
		// ZookeeperUtil.createNode("/cheng","xcg_");
		// ZookeeperUtil.getData("/cheng");
		// ZookeeperUtil.setData("/cheng", "abcde");
		// ZookeeperUtil.delete("/hive_zookeeper_namespace/");
		// ZookeeperUtil.getData("/cheng");
		//ZookeeperUtil.list("/hiveserver2");
		// ZookeeperUtil.createNode("/cheng");
		// ZookeeperUtil.delete("/cheng");
		// ZookeeperUtil.getACL("/cheng");
		// ZookeeperUtil.close();
		//ZookeeperUtil.setData("/cheng", "abcde");
		ZookeeperUtil.getData("/cheng");
		Thread.currentThread().sleep(Long.MAX_VALUE);
		
	}

}

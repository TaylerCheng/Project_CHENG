package com.cg.zookeeper;

import java.util.List;

import org.apache.zookeeper.KeeperException;

public class ListGroup extends ConnectionWatcher {

	private void list(String groupName) {
		try {
			List<String> children = zk.getChildren(groupName, false);
			if (children.isEmpty()) {
				System.out.println("is empty");
			}
			for (String child : children) {
				System.out.println(child);
				byte[] data = zk.getData(groupName+"/"+child, this, null);
				System.out.println(data);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		ListGroup listGroup = new ListGroup();
		listGroup.connect("master.hadoop:2181");
		listGroup.list("/dubbo");
		listGroup.close();
	}
}

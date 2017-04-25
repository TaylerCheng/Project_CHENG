package com.cg.zookeeper;

import java.util.List;

import org.apache.zookeeper.KeeperException;

public class ListGroup extends ConnectionWatcher {

    private void list(String groupName) {
        try {
            List<String> children = zk.getChildren(groupName, false);
            if (!children.isEmpty()) {
                for (String child : children) {
                    if (!groupName.endsWith("/")) {
                        groupName += "/";
                    }
                    String path = groupName + child;
                    byte[] data = zk.getData(path, this, null);
                    if (data == null) {
                        System.out.print("(D)" + path);
                    } else {
                        System.out.print( path + " -> " + new String(data));
                    }
                    System.out.println();
                    list(path);
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ListGroup listGroup = new ListGroup();
        listGroup.connect("slave1.hadoop:2181");
        listGroup.list("/brokers");
        listGroup.list("/cluster");
        listGroup.list("/consumers");
        listGroup.list("/config");
        listGroup.close();
    }
}

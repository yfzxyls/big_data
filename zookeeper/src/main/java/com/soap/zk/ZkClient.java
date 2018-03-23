package com.soap.zk;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by yangf on 2018/3/20.
 */
public class ZkClient {

    private String zk = "hadoop200:2181,hadoop201:2181,hadoop202:2181";
    ZooKeeper zooKeeper = null;

    @Before
    public void init() {
        try {
            zooKeeper = new ZooKeeper(zk, 4000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent.getType() + "--" + watchedEvent.getState());

                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("zooKeeper:" + zooKeeper);
    }

    @Test
    public void createNode() {
        try {
            String string = zooKeeper.create("/soap/yang", "yangzong".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(string);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getChild() {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren("/soap", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.err.println("stat:" + watchedEvent.getState());
                }
            });
            Thread.sleep(6000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (String string : children) {
            System.out.println(string);
        }
    }

    @Test
    public void isChildExist() throws KeeperException, InterruptedException {
        zooKeeper.exists("/soap", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("stat : " + watchedEvent.getState());
            }
        });
    }
}

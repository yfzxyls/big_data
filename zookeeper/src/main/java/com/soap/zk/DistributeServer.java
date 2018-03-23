package com.soap.zk;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/20.
 */
public class DistributeServer {

    private String zk = "hadoop200:2181,hadoop201:2181,hadoop202:2181";
    private ZooKeeper zooKeeper = null;

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
    }

    public void online(String hostname){
        try {
          String create = zooKeeper.create("/servers/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(hostname +" is noline "+ create);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 业务功能
    public void business(String hostname) throws Exception{
        System.out.println(hostname+" is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        // 获取zk连接
        DistributeServer server = new DistributeServer();
        server.init();

        // 利用zk连接注册服务器信息
        server.online(args[0]);

        // 启动业务功能
        server.business(args[0]);
    }
}

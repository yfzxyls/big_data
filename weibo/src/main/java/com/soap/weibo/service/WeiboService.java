package com.soap.weibo.service;

import com.soap.big_data.hbase.util.HBaseUtil;
import com.soap.weibo.constant.WeiboTableConstant;
import com.soap.weibo.vo.MessageVo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

/**
 * Created by yangf on 2018/3/17.
 */
@Service
public class WeiboService {

    @Resource
    private Configuration conf;

    /**
     * 初始化数据：
     * 1.创建命名空间 ns_weibo
     * 2.创建微博内容表 ns_weibo:content    rowkey:1001_1545454545 info:content = 北京下雪列
     * 3.创建用户关系表 ns_weibo:relation rowkey:1001   columnFamily:attend fans
     * 4.创建用户收件箱 ns_weibo:inbox  columnFamily:info column:uid value:content.rowkey
     */
    public void init() throws Exception {
        HBaseUtil.createNameSpace(conf,
                WeiboTableConstant.WEIBO_NAMESPACE);
        HBaseUtil.createTable(conf,
                WeiboTableConstant.WEIBO_CONTENT,
                1,
                WeiboTableConstant.WEIBO_CONTENT_COLUMN_FAMILY);
        HBaseUtil.createTable(conf,
                WeiboTableConstant.WEIBO_RELATION,
                1,
                WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_ATTEND,
                WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_FANS);
        HBaseUtil.createTable(conf,
                WeiboTableConstant.WEIBO_INBOX,
                1000,
                WeiboTableConstant.WEIBO_INBOX_INFO);
    }


    /**
     * 1.向微博content中插入数据
     * 2.从relation中找到发送人的粉丝的uid
     * 3.根据uid在inbox中插入数据
     *
     * @param uid
     * @param content
     */
    public void publishContent(String uid, String content) {
        long timestamp = System.currentTimeMillis();
        String contentRowkey = uid + "_" + timestamp;
        HBaseUtil.addRow(conf,
                WeiboTableConstant.WEIBO_CONTENT,
                contentRowkey,
                WeiboTableConstant.WEIBO_CONTENT_COLUMN_FAMILY,
                WeiboTableConstant.WEIBO_CONTENT_COLUMN_FAMILY,
                content);
        Map<String, String> fansUids = HBaseUtil.getRowMultiColumns(conf,
                WeiboTableConstant.WEIBO_RELATION,
                uid,
                WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_FANS,0);
        List<Put> puts = new ArrayList<>();
        for (Map.Entry<String, String> fansUid : fansUids.entrySet()) {
            Put put = new Put(Bytes.toBytes(fansUid.getKey()));
            put.addColumn(Bytes.toBytes(WeiboTableConstant.WEIBO_INBOX_INFO),
                    Bytes.toBytes(uid), Bytes.toBytes(contentRowkey));
            puts.add(put);
        }
        Put put = new Put(Bytes.toBytes(uid)).addColumn(Bytes.toBytes(WeiboTableConstant.WEIBO_INBOX_INFO)
                ,Bytes.toBytes(uid), Bytes.toBytes(contentRowkey));
        puts.add(put);
        
        HBaseUtil.addByPutList(conf,
                WeiboTableConstant.WEIBO_INBOX,
                puts);
    }


    /**
     * 1.在关系表中插入数据
     * 2.查找关注人已经添加的微博rowkeys
     * 3.将rowkeys添加的关注人的收件箱中
     *
     * @param uid
     * @param attends
     */
    public void addAttends(String uid, String... attends) {
        //1.
        List<Put> relationPuts = new ArrayList<>();
        for (int i = 0; i < attends.length; i++) {
            if (!uid.equals(attends[i])) {
                //我关注的添加数据
                Put attendPut = new Put(Bytes.toBytes(uid))
                        .addColumn(Bytes.toBytes(WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_ATTEND),
                                Bytes.toBytes(attends[i]), null);
                //我关注的人添加粉丝
                relationPuts.add(attendPut);
                Put fansPut = new Put(Bytes.toBytes(attends[i]))
                        .addColumn(Bytes.toBytes(WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_FANS),
                                Bytes.toBytes(uid), null);
                relationPuts.add(fansPut);
            }
        }
        HBaseUtil.addByPutList(conf, WeiboTableConstant.WEIBO_RELATION, relationPuts);

        //2
        List<String> rowkeys = getConentRowKeys(uid, attends);

        //3.
        List<Put> inboxPuts = new ArrayList<>();
        for (Iterator<String> iterator = rowkeys.iterator(); iterator.hasNext(); ) {
            String rowkey = iterator.next();
            String publishUid = rowkey.substring(0, rowkey.indexOf("_"));
            Put put = new Put(Bytes.toBytes(uid))
                    .addColumn(Bytes.toBytes(WeiboTableConstant.WEIBO_INBOX_INFO),
                            Bytes.toBytes(publishUid), Bytes.toBytes(rowkey));
            inboxPuts.add(put);
        }
        HBaseUtil.addByPutList(conf, WeiboTableConstant.WEIBO_INBOX, inboxPuts);
    }


    /**
     * 1.删除微博内容中的数据
     * 2.查找该微博发布者粉丝得uid
     * 3.删除uid收件箱中的column为uid的微博rowkey
     *
     * @param uid
     * @param rowKey
     */
    public void delContent(String uid, String rowKey) {
        //1.
        HBaseUtil.deleteRow(conf, WeiboTableConstant.WEIBO_CONTENT, rowKey);

        //2.
        Map<String, String> fansUids = HBaseUtil.getRowMultiColumns(conf,
                WeiboTableConstant.WEIBO_RELATION,
                uid,
                WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_FANS,0,
                null);

        List<Delete> deletes = new ArrayList<>();
        for (Map.Entry<String, String> fansUid : fansUids.entrySet()) {
            Delete delete = new Delete(Bytes.toBytes(fansUid.getKey()));
        }
    }

    /**
     * 取消关注
     *
     * @param uid
     * @param attends
     */
    public void deleteAttends(String uid, String... attends) {
        List<Delete> relationAttend = new ArrayList<>();
        for (int i = 0; i < attends.length; i++) {
            String attend = attends[i];
            //删除我关注的人
            Delete deleteAttend = new Delete(Bytes.toBytes(uid))
                    .addColumns(Bytes.toBytes(WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_ATTEND),
                            Bytes.toBytes(attend));
            relationAttend.add(deleteAttend);
            //删除我关注人的粉丝
            Delete fansDel = new Delete(Bytes.toBytes(attend))
                    .addColumns(Bytes.toBytes(WeiboTableConstant.WEIBO_RELATION_COLUMN_FAMILY_FANS),
                            Bytes.toBytes(uid));
            relationAttend.add(fansDel);
        }
        HBaseUtil.deleteByDeletes(conf, WeiboTableConstant.WEIBO_RELATION, relationAttend);
//        List<String> rowKeys = getConentRowKeys(uid, attends);
        List<Delete> deleteList = new ArrayList<>();
        for (int i = 0; i < attends.length; i++) {
            Delete delete = new Delete(Bytes.toBytes(uid))
                    .addColumns(Bytes.toBytes(WeiboTableConstant.WEIBO_INBOX_INFO),
                            Bytes.toBytes(attends[i]));
            deleteList.add(delete);
        }
        HBaseUtil.deleteByDeletes(conf, WeiboTableConstant.WEIBO_INBOX, deleteList);
    }

    private List<String> getConentRowKeys(String uid, String[] attends) {
        List<Scan> scans = new ArrayList<>();
        for (int i = 0; i < attends.length; i++) {
            if (!uid.equals(attends[i])) {
                Scan scan = new Scan();
                Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attends[i] + "_"));
                scan.setFilter(filter);
                scans.add(scan);
            }
        }

        return HBaseUtil.getRowkeysByScans(conf, WeiboTableConstant.WEIBO_CONTENT, scans, 0);
    }

    /**
     * 获取微博消息
     * 1.在收件箱中查找微博rowkeys
     * 2.根据rowkeys 去查找微博内容
     *
     * @param uid
     * @return
     */
    public List<MessageVo> getContentList(String uid) {

        Map<String, String> contentRowkeys = HBaseUtil.getRowMultiColumns(conf,
                WeiboTableConstant.WEIBO_INBOX,
                uid,
                null,Integer.MAX_VALUE, null);
        List<Get> gets = new ArrayList<>();
        for (Map.Entry<String, String> contentRowkey : contentRowkeys.entrySet()) {
            Get get = new Get(Bytes.toBytes(contentRowkey.getValue()));
            gets.add(get);
        }
        Map<String, Map<String, String>> results = HBaseUtil.getRowByGets(conf, WeiboTableConstant.WEIBO_CONTENT, gets);
        List<MessageVo> messageVos = new ArrayList<>();
        if (results == null || results.size() <= 0) return messageVos;
        for (Map.Entry<String, Map<String, String>> result : results.entrySet()) {
            MessageVo messageVo = new MessageVo();
            String rowKey = result.getKey();
            messageVo.setRowKey(rowKey);
            messageVo.setUid(rowKey.substring(0, rowKey.indexOf("_")));
            messageVo.setPubDate(new DateTime(Long.valueOf(rowKey.substring(rowKey.indexOf("_") + 1))).toString("yyyy-MM-dd HH:mm:ss"));
            messageVo.setContent(result.getValue().get(WeiboTableConstant.WEIBO_CONTENT_COLUMN_FAMILY));
            messageVos.add(messageVo);
        }
        return messageVos;
    }
}

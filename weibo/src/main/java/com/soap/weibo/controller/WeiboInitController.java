package com.soap.weibo.controller;

import com.soap.weibo.service.WeiboService;
import com.soap.weibo.vo.MessageVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangf on 2018/3/17.
 */
@RestController("weibo")
@Api(description = "微博接口")
public class WeiboInitController {


    @Resource
    private WeiboService weiboService;



    @GetMapping("init")
    @ApiOperation(value = "初始化数据")
    public String init() {
        try {
            weiboService.init();
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "fail";
    }

    @PutMapping("publish_weibo")
    @ApiOperation(value = "发布消息")
    public String publishWeibo(String uid,String conten) {
        try {
            weiboService.publishContent(uid,conten);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "fail";
    }

    @PutMapping("add_attend")
    @ApiOperation(value = "添加关注")
    public String addAttend(String uid,String ... attends) {
        try {
            weiboService.addAttends(uid,attends);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "fail";
    }

    @PutMapping("del_attend")
    @ApiOperation(value = "取消关注")
    public String delAttend(String uid,String ... attends) {
        try {
            weiboService.deleteAttends(uid,attends);
            return "success";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "fail";
    }

    @GetMapping("get_content_list")
    @ApiOperation(value = "获取微博")
    public List<MessageVo> getContentList(String uid) {
        try {
           return weiboService.getContentList(uid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }
    

}

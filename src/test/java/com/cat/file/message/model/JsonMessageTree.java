package com.cat.file.message.model;

import com.alibaba.fastjson.JSONObject;
import com.cat.file.message.internal.AbstractMessageTree;
import com.cat.file.message.internal.DefaultMessageTree;
import com.cat.file.message.internal.MessageTree;

import java.util.Date;

/**
 * @Module TODO
 * @Description TODO
 * @Author liukaixiong
 * @Date 2020/11/16 18:10
 */
public class JsonMessageTree extends AbstractMessageTree<JSONObject> {


    private JSONObject message;

    public JsonMessageTree(String domain, Integer id, Date createTime, JSONObject message) {
        super(domain, id, createTime, message);
    }


    @Override
    public JSONObject getMessage() {
        return message;
    }

    @Override
    public void setMessage(JSONObject message) {
        this.message = message;
    }
}

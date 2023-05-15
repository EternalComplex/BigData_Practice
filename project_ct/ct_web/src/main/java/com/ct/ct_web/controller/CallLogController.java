package com.ct.ct_web.controller;

import com.ct.ct_web.service.CallLogService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@RestController
public class CallLogController {
    @Resource
    private CallLogService callLogService;

    @RequestMapping("/query")
    public Map<String, String> query() {
        Map<String, String> map = new HashMap<>();
        map.put("name", "zhangSan");
        map.put("age", "18");

        return map;
    }
}

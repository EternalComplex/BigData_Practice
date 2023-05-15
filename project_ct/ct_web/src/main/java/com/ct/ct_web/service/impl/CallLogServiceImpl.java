package com.ct.ct_web.service.impl;

import com.ct.ct_web.mapper.CallLogMapper;
import com.ct.ct_web.service.CallLogService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class CallLogServiceImpl implements CallLogService {

    @Resource
    private CallLogMapper callLogMapper;
}

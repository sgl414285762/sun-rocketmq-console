/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.console.controller;

import com.google.common.collect.Maps;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.console.model.MessageView;
import org.apache.rocketmq.console.model.trace.MessageTraceGraph;
import org.apache.rocketmq.console.service.MessageService;
import org.apache.rocketmq.console.service.MessageTraceService;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/messageTrace")
public class MessageTraceController {

    @Resource
    private MessageService messageService;

    @Resource
    private MessageTraceService messageTraceService;

    @RequestMapping(value = "/viewMessage.query", method = RequestMethod.GET)
    @ResponseBody
    public Object viewMessage(@RequestParam(required = false) String topic, @RequestParam String msgId) {
        Map<String, Object> messageViewMap = Maps.newHashMap();
        Pair<MessageView, List<MessageTrack>> messageViewListPair = messageService.viewMessage(topic, msgId);
        messageViewMap.put("messageView", messageViewListPair.getObject1());
        return messageViewMap;
    }

    /**
     * @Description: 根据messageKey查询消息轨迹（系统默认的topic）
     * @Author shenguoliang
     * @param msgKey
     * @Date 2021/9/16 16:31
     * @Return java.lang.Object
     * @ModificationHistory
     *
     */
    @RequestMapping(value = "/viewMessageTraceDetail.queryKey", method = RequestMethod.GET)
    @ResponseBody
    public MessageTraceGraph viewTraceMessagesByKey(@RequestParam String msgKey) {
        return messageTraceService.queryMessageTraceGraph(msgKey);
    }
    @RequestMapping(value = "/viewMessageTraceDetail.query", method = RequestMethod.GET)
    @ResponseBody
    public Object viewTraceMessages(@RequestParam String msgId) {
        return messageTraceService.queryMessageTraceKey(msgId);
    }

    /**
     * @Description: 根据messageId查询消息轨迹（topic需要选择对应的broker的主题，而非系统默认的topic）
     * @Author shenguoliang
     * @param msgId
     * @Date 2021/9/16 16:32
     * @Return org.apache.rocketmq.console.model.trace.MessageTraceGraph
     * @ModificationHistory
     *
     */
    @RequestMapping(value = "/viewMessageTraceGraph.query", method = RequestMethod.GET)
    @ResponseBody
    public MessageTraceGraph viewMessageTraceGraph(@RequestParam String msgId) {
        return messageTraceService.queryMessageTraceGraph(msgId);
    }
}

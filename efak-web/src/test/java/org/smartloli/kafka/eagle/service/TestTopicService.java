/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartloli.kafka.eagle.service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSON;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.smartloli.kafka.eagle.common.util.CalendarUtils;
import org.smartloli.kafka.eagle.common.util.KConstants;
import org.smartloli.kafka.eagle.common.util.KafkaCacheUtils;
import org.smartloli.kafka.eagle.web.controller.TopicController;
import org.smartloli.kafka.eagle.web.service.TopicService;
import org.smartloli.kafka.eagle.web.service.impl.TopicServiceImpl;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
* Test TopicService clazz.
* 
* @author smartloli.
*
* Created by Jan 16, 2017
*/
public class TestTopicService {

	@InjectMocks
	private TopicController topicController;

	@Mock
	private TopicService topicService;

	@Before
	public void setup() throws NoSuchFieldException, IllegalAccessException {
		MockitoAnnotations.openMocks(this);
		topicController = new TopicController();
		topicService = new TopicServiceImpl();
		Field serviceField = TopicController.class.getDeclaredField("topicService");
		serviceField.setAccessible(true);
		serviceField.set(topicController, topicService);
		KafkaCacheUtils.initKafkaMetaData();
	}

	public static void main(String[] args) {
		System.out.println();
	}

	@Test
	public void testTopicMessageAjax() throws Exception {
		// Arrange
		String clusterAlias = "cluster1";
		String tname = "testTopic";
		String stime = "20250101080000";
		String etime = "20250731080000";
		int partition = 0;
		int page = 1;
		boolean needCount = true;

		HttpServletRequest request = mock(HttpServletRequest.class);
		HttpServletResponse response = mock(HttpServletResponse.class);
		HttpSession session = mock(HttpSession.class);

		JSONObject tmp = new JSONObject();
		tmp.put("topic", tname);
		tmp.put("stime", CalendarUtils.convertUnsplitDateTimeToUnixTime(stime));
		tmp.put("etime", CalendarUtils.convertUnsplitDateTimeToUnixTime(etime));
		tmp.put("partition", partition);
		tmp.put("page", page);
		tmp.put("need_count", needCount);

		when(request.getParameter("stime")).thenReturn(stime);
		when(request.getParameter("etime")).thenReturn(etime);
		when(request.getParameter("partition")).thenReturn(String.valueOf(partition));
		when(request.getParameter("page")).thenReturn(String.valueOf(page));
		when(request.getParameter("need_count")).thenReturn(String.valueOf(needCount));
		when(request.getSession()).thenReturn(session);
		when(session.getAttribute(anyString())).thenReturn(clusterAlias);

		ByteArrayOutputStream outputContent = new ByteArrayOutputStream();
		when(response.getOutputStream()).thenReturn(new MockServletOutputStream(outputContent));

		// Act
		topicController.topicMessageAjax(tname, response, request);
	}
}

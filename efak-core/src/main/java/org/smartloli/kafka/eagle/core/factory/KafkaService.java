/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartloli.kafka.eagle.core.factory;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.TopicPartition;
import org.smartloli.kafka.eagle.common.protocol.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka group,topic and partition interface.
 *
 * @author smartloli.
 * <p>
 * Created by Jan 18, 2017
 * <p>
 * Update by hexiang 20170216
 */
public interface KafkaService {

    /**
     * Find topic and group exist in zookeeper.
     */
    public boolean findTopicAndGroupExist(String clusterAlias, String topic, String group);

    /**
     * Obtaining metadata in zookeeper by topic.
     */
    public List<String> findTopicPartition(String clusterAlias, String topic);

    /**
     * Get kafka active consumer topic.
     */
    public Map<String, List<String>> getActiveTopic(String clusterAlias);

    /**
     * Get kafka active consumer topic.
     */
    public Set<String> getActiveTopic(String clusterAlias, String group);

    /**
     * Get all broker list from zookeeper.
     */
    public List<BrokersInfo> getAllBrokersInfo(String clusterAlias);

    /**
     * Get broker host info from ids.
     */
    public String getBrokerJMXFromIds(String clusterAlias, int ids);

    /**
     * Obtaining kafka consumer information from zookeeper.
     */
    public Map<String, List<String>> getConsumers(String clusterAlias);

    /**
     * Get kafka 0.10.x offset from topic.
     */
    public String getKafkaOffset(String clusterAlias);

    /**
     * Get the data for the topic partition in the specified consumer group
     */
    public Map<Integer, Long> getKafkaOffset(String clusterAlias, String group, String topic, Set<Integer> partitionids);

    /**
     * Use kafka console comand to create topic.
     */
    public Map<String, Object> create(String clusterAlias, String topicName, String partitions, String replic);

    /**
     * Use kafka console command to delete topic.
     */
    public Map<String, Object> delete(String clusterAlias, String topicName);

    /**
     * Convert query kafka to topic in the sql message for standard sql.
     */
    public KafkaSqlInfo parseSql(String clusterAlias, String sql);

    /**
     * Get kafka 0.10.x active consumer group & topics.
     */
    public Set<String> getKafkaActiverTopics(String clusterAlias, String group);

    /**
     * Get kafka 0.10.x consumer topic, maybe consumer topic owner is null.
     */
    public Set<String> getKafkaConsumerTopics(String clusterAlias, String group);

    /**
     * Get kafka 0.10.x consumer group & topic information.
     */
    public String getKafkaConsumer(String clusterAlias);

    /**
     * Get consumer group info by distribute mode.
     */
    public String getDistributeKafkaConsumer(String clusterAlias);

    /**
     * Get kafka 0.10.x consumer group & topic information used for page.
     */
    public String getKafkaConsumer(String clusterAlias, DisplayInfo page);

    @Deprecated
    /** Get kafka consumer information pages. */
    public String getKafkaActiverSize(String clusterAlias, String group);

    /**
     * Get kafka consumer information pages not owners.
     */
    public OwnerInfo getKafkaActiverNotOwners(String clusterAlias, String group);

    /**
     * Get kafka broker bootstrap server.
     */
    public String getKafkaBrokerServer(String clusterAlias);

    /**
     * Get kafka consumer groups.
     */
    public int getKafkaConsumerGroups(String clusterAlias);

    /**
     * Get kafka consumer topics.
     */
    public Set<String> getKafkaConsumerTopic(String clusterAlias, String group);

    /**
     * Get kafka consumer group & topic.
     */
    public String getKafkaConsumerGroupTopic(String clusterAlias, String group);

    /**
     * Get kafka topic history logsize .
     */
    public long getKafkaLogSize(String clusterAlias, String topic, int partitionid);

    /**
     * Get kafka topic history batch logsize.
     */
    public Map<TopicPartition, Long> getKafkaLogSize(String clusterAlias, String topic, Set<Integer> partitionids);

    /**
     * Get kafka topic real logsize by partitionid.
     */
    public long getKafkaRealLogSize(String clusterAlias, String topic, int partitionid);

    /**
     * Get kafka topic real logsize by partitionid set.
     */
    public long getKafkaRealLogSize(String clusterAlias, String topic, Set<Integer> partitionids);

    /**
     * Get topic producer send logsize records.
     */
    public long getKafkaProducerLogSize(String clusterAlias, String topic, Set<Integer> partitionids);

    /**
     * Get kafka sasl topic metadate.
     */
    public List<MetadataInfo> findKafkaLeader(String clusterAlias, String topic);

    /**
     * Send mock message to kafka.
     */
    public boolean mockMessage(String clusterAlias, String topic, String message);

    /**
     * Get kafka consumer group all topics lag.
     */
    public long getKafkaLag(String clusterAlias, String group, String topic);

    /**
     * Get topic metadata.
     */
    public String getReplicasIsr(String clusterAlias, String topic, int partitionid);

    /**
     * Get kafka version.
     */
    public String getKafkaVersion(String host, int port, String ids, String clusterAlias);

    /**
     * Get kafka os memory.
     */
    public long getOSMemory(String clusterAlias, String host, int port, String property);

    /**
     * Get kafka used cpu.
     */
    public String getUsedCpu(String clusterAlias, String host, int port);

    /**
     * Get kafka used cpu value.
     */
    public double getUsedCpuValue(String clusterAlias, String host, int port);

    /**
     * Get kafka used memory.
     */
    public String getUsedMemory(String clusterAlias, String host, int port);

    /**
     * Set kafka sasl acl.
     */
    public void sasl(Properties props, String clusterAlias);

    /**
     * Set kafka ssl acl.
     */
    public void ssl(Properties props, String clusterAlias);

    /**
     * Preview topic and partition message.
     */
    public String getPreviewTopicPartitionMsg(String clusterAlias, JSONObject tp);

    /**
     * List filtered topic message
     */
    public JSONObject listTopicMessage(String clusterAlias, JSONObject tmp);
}

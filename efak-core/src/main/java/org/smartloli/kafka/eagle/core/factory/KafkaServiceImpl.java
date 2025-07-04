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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartloli.kafka.eagle.common.constant.JmxConstants.BrokerServer;
import org.smartloli.kafka.eagle.common.constant.JmxConstants.KafkaServer8;
import org.smartloli.kafka.eagle.common.constant.KSqlParser;
import org.smartloli.kafka.eagle.common.protocol.*;
import org.smartloli.kafka.eagle.common.protocol.cache.BrokerCache;
import org.smartloli.kafka.eagle.common.protocol.topic.TopicPartitionSchema;
import org.smartloli.kafka.eagle.common.util.*;
import org.smartloli.kafka.eagle.common.util.KConstants.BrokerSever;
import org.smartloli.kafka.eagle.common.util.KConstants.CollectorType;
import org.smartloli.kafka.eagle.common.util.KConstants.Kafka;
import org.smartloli.kafka.eagle.common.util.kraft.KafkaSchemaFactory;
import org.smartloli.kafka.eagle.common.util.kraft.KafkaStoragePlugin;
import org.smartloli.kafka.eagle.core.sql.execute.KafkaConsumerAdapter;
import org.smartloli.kafka.eagle.core.task.strategy.WorkNodeStrategy;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Implements KafkaService all method.
 *
 * @author smartloli.
 * <p>
 * Created by Aug 3, 2022.
 */
public class KafkaServiceImpl implements KafkaService {

    private final String BROKER_IDS_PATH = "/brokers/ids";
    private final String BROKER_TOPICS_PATH = "/brokers/topics";
    private final String CONSUMERS_PATH = "/consumers";
    private final String OWNERS = "/owners";
    private final String TOPIC_ISR = "/brokers/topics/%s/partitions/%s/state";
    private final Logger LOG = LoggerFactory.getLogger(KafkaServiceImpl.class);

    /**
     * Instance Kafka Zookeeper client pool.
     */
    // private KafkaZKPoolUtils kafkaZKPool = KafkaZKPoolUtils.getInstance();
    private KafkaZKPoolUtils kafkaZKPool = KafkaZKSingletonUtils.create();

    /**
     * Zookeeper service interface.
     */
    private ZkService zkService = new ZkFactory().create();

    /**
     * Find topic and group exist in zookeeper.
     *
     * @param topic Filter topic.
     * @param group Filter group
     * @return Boolean.
     */
    public boolean findTopicAndGroupExist(String clusterAlias, String topic, String group) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        String ownersPath = CONSUMERS_PATH + "/" + group + "/owners/" + topic;
        boolean status = false;
        try {
            status = zkc.pathExists(ownersPath);
        } catch (Exception e) {
            LoggerUtils.print(this.getClass()).error("Find topic and group exist has error, msg is ", e);
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return status;
    }

    /**
     * Obtaining metadata in zookeeper by topic.
     *
     * @param topic Selected condition.
     * @return List.
     */
    @Override
    public List<String> findTopicPartition(String clusterAlias, String topic) {
//        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
//        List<String> topicAndPartitions = null;
//        Seq<String> brokerTopicsPaths = null;
//        try {
//            brokerTopicsPaths = zkc.getChildren(BROKER_TOPICS_PATH + "/" + topic + "/partitions");
//            topicAndPartitions = JavaConversions.seqAsJavaList(brokerTopicsPaths);
//        } catch (Exception e) {
//            LoggerUtils.print(this.getClass()).error("Find topic partition has error, msg is ", e);
//        } finally {
//            if (zkc != null) {
//                kafkaZKPool.release(clusterAlias, zkc);
//                zkc = null;
//                brokerTopicsPaths = null;
//            }
//        }
//        return topicAndPartitions;

        KafkaSchemaFactory ksf = new KafkaSchemaFactory(new KafkaStoragePlugin());
        return ksf.getTopicPartitionsOfString(clusterAlias, topic);
    }

    /**
     * Get kafka active consumer topic.
     */
    public Map<String, List<String>> getActiveTopic(String clusterAlias) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        // k : v = group_topic : List<Topic>
        // [
        //    g1_t1 : {t1},
        //    g1_t2 : {t2},
        //    g1_t3 : {t3},
        //    g2_t2 : {t2},
        //    g2_t5 : {t5},
        //    g2_t7 : {t7},
        // ]
        Map<String, List<String>> actvTopics = new HashMap<String, List<String>>();
        try {
            Seq<String> subConsumerPaths = zkc.getChildren(CONSUMERS_PATH);
            List<String> groups = JavaConversions.seqAsJavaList(subConsumerPaths);
            // groupsAndTopics :
            // [
            //    { topic :  t1 , group : g1}
            //    { topic :  t2 , group : g1}
            //    { topic :  t3 , group : g1}
            //    { topic :  t2 , group : g2}
            //    { topic :  t5 , group : g2}
            //    { topic :  t7 , group : g2}
            // ]
            JSONArray groupsAndTopics = new JSONArray();
            for (String group : groups) {
                Seq<String> topics = zkc.getChildren(CONSUMERS_PATH + "/" + group + OWNERS);
                for (String topic : JavaConversions.seqAsJavaList(topics)) {
                    Seq<String> partitionIds = zkc.getChildren(CONSUMERS_PATH + "/" + group + OWNERS + "/" + topic);
                    if (JavaConversions.seqAsJavaList(partitionIds).size() > 0) {
                        JSONObject groupAndTopic = new JSONObject();
                        groupAndTopic.put("topic", topic);
                        groupAndTopic.put("group", group);
                        groupsAndTopics.add(groupAndTopic);
                    }
                }
            }
            for (Object object : groupsAndTopics) {
                JSONObject groupAndTopic = (JSONObject) object;
                String group = groupAndTopic.getString("group");
                String topic = groupAndTopic.getString("topic");
                if (actvTopics.containsKey(group + "_" + topic)) {
                    actvTopics.get(group + "_" + topic).add(topic);
                } else {
                    List<String> topics = new ArrayList<String>();
                    topics.add(topic);
                    actvTopics.put(group + "_" + topic, topics);
                }
            }
        } catch (Exception e) {
            LoggerUtils.print(this.getClass()).error("Get active topic has error, msg is ", e);
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return actvTopics;
    }

    /**
     * Get kafka active consumer topic.
     */
    public Set<String> getActiveTopic(String clusterAlias, String group) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        Set<String> activeTopics = new HashSet<>();
        try {
            Seq<String> topics = zkc.getChildren(CONSUMERS_PATH + "/" + group + OWNERS);
            for (String topic : JavaConversions.seqAsJavaList(topics)) {
                activeTopics.add(topic);
            }
        } catch (Exception e) {
            LOG.error("Get kafka active topic has error, msg is ", e);
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return activeTopics;
    }

    /**
     * Get all broker list from zookeeper.
     */
    public List<BrokersInfo> getAllBrokersInfo(String clusterAlias) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        List<BrokersInfo> targets = new ArrayList<BrokersInfo>();
        try {
            if (zkc.pathExists(BROKER_IDS_PATH)) {
                Seq<String> subBrokerIdsPaths = zkc.getChildren(BROKER_IDS_PATH);
                List<String> brokerIdss = JavaConversions.seqAsJavaList(subBrokerIdsPaths);
                int id = 0;
                for (String ids : brokerIdss) {
                    try {
                        Tuple2<Option<byte[]>, Stat> tuple = zkc.getDataAndStat(BROKER_IDS_PATH + "/" + ids);
                        BrokersInfo broker = new BrokersInfo();
                        broker.setCreated(CalendarUtils.convertUnixTime2Date(tuple._2.getCtime()));
                        broker.setModify(CalendarUtils.convertUnixTime2Date(tuple._2.getMtime()));
                        String tupleString = new String(tuple._1.get());
                        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable") || SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
                            String endpoints = JSON.parseObject(tupleString).getString("endpoints");
                            List<String> endpointsList = JSON.parseArray(endpoints, String.class);
                            String host = "";
                            int port = 0;
                            if (endpointsList.size() > 1) {
                                String protocol = "";
                                if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
                                    protocol = Kafka.SASL_PLAINTEXT;
                                }
                                if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
                                    protocol = Kafka.SSL;
                                }
                                for (String endpointsStr : endpointsList) {
                                    if (endpointsStr.contains(protocol)) {
                                        String tmp = endpointsStr.split("//")[1];
                                        host = tmp.split(":")[0];
                                        port = Integer.parseInt(tmp.split(":")[1]);
                                        break;
                                    }
                                }
                            } else {
                                if (endpointsList.size() > 0) {
                                    String tmp = endpointsList.get(0).split("//")[1];
                                    host = tmp.split(":")[0];
                                    port = Integer.parseInt(tmp.split(":")[1]);
                                }
                            }
                            broker.setHost(host);
                            broker.setPort(port);
                        } else {
                            String host = JSON.parseObject(tupleString).getString("host");
                            int port = JSON.parseObject(tupleString).getInteger("port");
                            broker.setHost(host);
                            broker.setPort(port);
                        }
                        broker.setJmxPort(JSON.parseObject(tupleString).getInteger("jmx_port"));
                        broker.setId(++id);
                        broker.setIds(ids);
                        try {
                            broker.setJmxPortStatus(NetUtils.telnet(broker.getHost(), broker.getJmxPort()));
                        } catch (Exception e) {
                            LOG.error("Telnet [" + broker.getHost() + ":" + broker.getJmxPort() + "] has error, msg is ", e);
                        }
                        targets.add(broker);
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            LoggerUtils.print(this.getClass()).error("Get all brokers info has error,msg is ", e);
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return targets;
    }

    /**
     * Obtaining kafka consumer information from zookeeper.
     */
    public Map<String, List<String>> getConsumers(String clusterAlias) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        Map<String, List<String>> consumers = new HashMap<String, List<String>>();
        try {
            Seq<String> subConsumerPaths = zkc.getChildren(CONSUMERS_PATH);
            List<String> groups = JavaConversions.seqAsJavaList(subConsumerPaths);
            for (String group : groups) {
                String path = CONSUMERS_PATH + "/" + group + "/owners";
                if (zkc.pathExists(path)) {
                    Seq<String> owners = zkc.getChildren(path);
                    List<String> ownersSerialize = JavaConversions.seqAsJavaList(owners);
                    consumers.put(group, ownersSerialize);
                } else {
                    LOG.error("Consumer Path[" + path + "] is not exist.");
                }
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return consumers;
    }

    /**
     * According to topic and partition to obtain Replicas & Isr.
     */
    public String getReplicasIsr(String clusterAlias, String topic, int partitionid) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        TopicPartition tp = new TopicPartition(topic, partitionid);
        List<Object> targets = null;
        try {
            Seq<Object> replis = zkc.getReplicasForPartition(tp);
            targets = JavaConversions.seqAsJavaList(replis);
        } catch (Exception e) {
            LoggerUtils.print(this.getClass()).error("Get topic replicas isr has error, msg is ", e);
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return targets.toString();
    }

    /**
     * Get zookeeper cluster information.
     */
    public String zkCluster(String clusterAlias) {
        String[] zks = SystemConfigUtils.getPropertyArray(clusterAlias + ".zk.list", ",");
        JSONArray targets = new JSONArray();
        int id = 1;
        for (String zk : zks) {
            JSONObject object = new JSONObject();
            object.put("id", id++);
            object.put("ip", zk.split(":")[0]);
            object.put("port", zk.split(":")[1]);
            object.put("mode", zkService.status(zk.split(":")[0], zk.split(":")[1]));
            targets.add(object);
        }
        return targets.toString();
    }

    /**
     * Judge whether the zkcli is active.
     */
    public JSONObject zkCliStatus(String clusterAlias) {
        JSONObject target = new JSONObject();
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        try {
            if (zkc != null) {
                target.put("live", true);
                target.put("list", SystemConfigUtils.getProperty(clusterAlias + ".zk.list"));
            } else {
                target.put("live", false);
                target.put("list", SystemConfigUtils.getProperty(clusterAlias + ".zk.list"));
            }
        } catch (Exception e) {
            LoggerUtils.print(this.getClass()).error("Get zookeeper client status has error,msg is ", e);
        } finally {
            if (zkc != null) {
                kafkaZKPool.release(clusterAlias, zkc);
                zkc = null;
            }
        }
        return target;
    }

    /**
     * Create topic to kafka cluster, it is worth noting that the backup number
     * must be less than or equal to brokers data.
     *
     * @param topicName  Create topic name.
     * @param partitions Create topic partitions.
     * @param replic     Replic numbers.
     * @return Map.
     */
    @Override
    public Map<String, Object> create(String clusterAlias, String topicName, String partitions, String replic) {
        Map<String, Object> targets = new HashMap<String, Object>();
        List<BrokersInfo> brokerLists = BrokerCache.META_CACHE.get(clusterAlias);
        int brokers = 0;
        if (brokerLists != null) {
            brokers = brokerLists.size();
        }
        if (Integer.parseInt(replic) > brokers) {
            targets.put("status", "error");
            targets.put("info", "replication factor: " + replic + " larger than available brokers: " + brokers);
            return targets;
        }
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            NewTopic newTopic = new NewTopic(topicName, Integer.valueOf(partitions), Short.valueOf(replic));
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (Exception e) {
            LoggerUtils.print(this.getClass()).error("Create kafka topic has error, msg is ", e);
        } finally {
            adminClient.close();
        }

        targets.put("status", "success");
        targets.put("info", "Create topic[" + topicName + "] has successed,partitions numbers is [" + partitions + "],replication-factor numbers is [" + replic + "]");
        return targets;
    }

    /**
     * Delete topic to kafka cluster.
     */
    public Map<String, Object> delete(String clusterAlias, String topicName) {
        Map<String, Object> targets = new HashMap<String, Object>();
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            adminClient.deleteTopics(Collections.singleton(topicName)).all().get();
            targets.put("status", "success");
        } catch (Exception e) {
            LOG.info("Delete kafka topic has error, msg is " + e.getMessage());
            e.printStackTrace();
            targets.put("status", "failed");
        } finally {
            adminClient.close();
        }
        return targets;
    }

    /**
     * Get kafka brokers from zookeeper.
     */
    private List<HostsInfo> getBrokers(String clusterAlias) {
        List<HostsInfo> targets = new ArrayList<HostsInfo>();
        List<BrokersInfo> brokers = getAllBrokersInfo(clusterAlias);
        for (BrokersInfo broker : brokers) {
            HostsInfo host = new HostsInfo();
            host.setHost(broker.getHost());
            host.setPort(broker.getPort());
            targets.add(host);
        }
        return targets;
    }

    private String parseBrokerServer(String clusterAlias) {
        String brokerServer = "";
        List<BrokersInfo> brokers = BrokerCache.META_CACHE.get(clusterAlias);
        for (BrokersInfo broker : brokers) {
            brokerServer += broker.getHost() + ":" + broker.getPort() + ",";
        }
        if ("".equals(brokerServer)) {
            return "";
        }
        return brokerServer.substring(0, brokerServer.length() - 1);
    }

    /**
     * Convert query sql to object.
     */
    public KafkaSqlInfo parseSql(String clusterAlias, String sql) {
        return segments(clusterAlias, prepare(sql));
    }

    private String prepare(String sql) {
        sql = sql.trim();
        sql = sql.replaceAll("\\s+", " ");
        return sql;
    }

    /**
     * Set topic sasl.
     */
    public void sasl(Properties props, String clusterAlias) {
        // configure the following four settings for SSL Encryption
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.sasl.protocol"));
        if (!"".equals(SystemConfigUtils.getProperty(clusterAlias + ".efak.sasl.client.id"))) {
            props.put(CommonClientConfigs.CLIENT_ID_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.sasl.client.id"));
        }
        props.put(SaslConfigs.SASL_MECHANISM, SystemConfigUtils.getProperty(clusterAlias + ".efak.sasl.mechanism"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.sasl.jaas.config"));
    }

    /**
     * Set topic ssl.
     */
    public void ssl(Properties props, String clusterAlias) {
        // configure the following three settings for SSL Encryption
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.protocol"));
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.truststore.location"));
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.truststore.password"));

        // configure the following three settings for SSL Authentication
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.keystore.location"));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.keystore.password"));
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.key.password"));

        // ssl handshake failed
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SystemConfigUtils.getProperty(clusterAlias + ".efak.ssl.endpoint.identification.algorithm"));

    }

    /***
     * Get preview topic message.
     * @param clusterAlias
     * @param tp
     * @return
     */
    public String getPreviewTopicPartitionMsg(String clusterAlias, JSONObject tp) {
        KafkaSqlInfo ksql = new KafkaSqlInfo();
        ksql.setClusterAlias(clusterAlias);
        ksql.setTableName(tp.getString("topic"));
        ksql.setPartition(Arrays.asList(tp.getInteger("partition")));
        return KafkaConsumerAdapter.preview(ksql).toString();
    }

    /**
     * List filtered topic message
     */
    @Override
    public JSONObject listTopicMessage(String clusterAlias, JSONObject tmp) {
        KafkaSqlInfo ksql = new KafkaSqlInfo();
        ksql.setClusterAlias(clusterAlias);
        ksql.setTopic(tmp.getString("topic"));
        ksql.setStartTime(tmp.getLong("stime"));
        ksql.setEndTime(tmp.getLong("etime"));
        ksql.setPartition(Collections.singletonList(tmp.getInteger("partition")));
        ksql.setPage(tmp.getInteger("page"));
        ksql.setLimit(KConstants.Kafka.MAX_POLL_RECORDS_NUM);
        ksql.setNeedCount(tmp.getBoolean("need_count"));
        return KafkaConsumerAdapter.filter(ksql);
    }

    private KafkaSqlInfo segments(String clusterAlias, String sql) {
        KafkaSqlInfo kafkaSql = new KafkaSqlInfo();
        kafkaSql.setSql(sql);
        kafkaSql.getSchema().put("partition", "integer");
        kafkaSql.getSchema().put("offset", "bigint");
        kafkaSql.getSchema().put("msg", "varchar");
        kafkaSql.getSchema().put("timespan", "varchar");
        kafkaSql.getSchema().put("date", "varchar");
        if (!sql.startsWith("select") && !sql.startsWith("SELECT")) {
            kafkaSql.setStatus(false);
            return kafkaSql;
        } else {
            TopicPartitionSchema tps = KSqlParser.parserTopic(sql);
            if (tps != null && !"".equals(tps.getTopic())) {
                kafkaSql.setStatus(true);
                kafkaSql.setTableName(tps.getTopic());
                kafkaSql.setSeeds(getBrokers(clusterAlias));
                kafkaSql.setPartition(tps.getPartitions());
                kafkaSql.setLimit(tps.getLimit());
            }
        }
        return kafkaSql;
    }

    /**
     * Get kafka 0.10.x after activer topics.
     */
    public Set<String> getKafkaActiverTopics(String clusterAlias, String group) {
        JSONArray consumerGroups = getKafkaMetadata(parseBrokerServer(clusterAlias), group, clusterAlias);
        Set<String> topics = new HashSet<>();
        for (Object object : consumerGroups) {
            JSONObject consumerGroup = (JSONObject) object;
            for (Object topicObject : consumerGroup.getJSONArray("topicSub")) {
                JSONObject topic = (JSONObject) topicObject;
                if (!"".equals(consumerGroup.getString("owner")) && consumerGroup.getString("owner") != null) {
                    topics.add(topic.getString("topic"));
                }
            }
        }
        return topics;
    }

    public Set<String> getKafkaConsumerTopics(String clusterAlias, String group) {
        JSONArray consumerGroups = getKafkaMetadata(parseBrokerServer(clusterAlias), group, clusterAlias);
        Set<String> topics = new HashSet<>();
        for (Object object : consumerGroups) {
            JSONObject consumerGroup = (JSONObject) object;
            for (Object topicObject : consumerGroup.getJSONArray("topicSub")) {
                JSONObject topic = (JSONObject) topicObject;
                topics.add(topic.getString("topic"));
            }
        }
        return topics;
    }

    /**
     * Get kafka 0.10.x, 1.x, 2.x consumer metadata.
     */
    public String getKafkaConsumer(String clusterAlias) {
        Properties prop = new Properties();
        JSONArray consumerGroups = new JSONArray();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            ListConsumerGroupsResult cgrs = adminClient.listConsumerGroups();
            java.util.Iterator<ConsumerGroupListing> itor = cgrs.all().get().iterator();
            while (itor.hasNext()) {
                ConsumerGroupListing gs = itor.next();
                JSONObject consumerGroup = new JSONObject();
                String groupId = gs.groupId();
                DescribeConsumerGroupsResult descConsumerGroup = adminClient.describeConsumerGroups(Arrays.asList(groupId));
                if (!groupId.contains("efak")) {
                    consumerGroup.put("group", groupId);
                    try {
                        Node node = descConsumerGroup.all().get().get(groupId).coordinator();
                        consumerGroup.put("node", node.host() + ":" + node.port());
                    } catch (Exception e) {
                        LOG.error("Get coordinator node has error, msg is " + e.getMessage());
                        e.printStackTrace();
                    }
                    consumerGroup.put("meta", getKafkaMetadata(parseBrokerServer(clusterAlias), groupId, clusterAlias));
                    consumerGroups.add(consumerGroup);
                }
            }
        } catch (Exception e) {
            LOG.error("Get kafka consumer has error,msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return consumerGroups.toString();
    }

    /**
     * Get kafka 0.10.x, 1.x, 2.x consumer metadata by distribute mode.
     */
    @Override
    public String getDistributeKafkaConsumer(String clusterAlias) {
        Properties prop = new Properties();
        JSONArray consumerGroups = new JSONArray();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        List<String> hosts = WorkUtils.getWorkNodes();
        int port = SystemConfigUtils.getIntProperty("efak.worknode.port");
        List<WorkNodeStrategy> nodes = new ArrayList<>();
        for (String host : hosts) {
            if (NetUtils.telnet(host, port)) {
                WorkNodeStrategy wns = new WorkNodeStrategy();
                wns.setPort(port);
                wns.setHost(host);
                String masterHost = SystemConfigUtils.getProperty("efak.worknode.master.host");
                if (!masterHost.equals(host)) {
                    nodes.add(wns);
                }
            }
        }

        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            ListConsumerGroupsResult cgrs = adminClient.listConsumerGroups();
            java.util.Iterator<ConsumerGroupListing> itor = cgrs.all().get().iterator();
            int nodeIndex = 0;
            String[] cgroups = SystemConfigUtils.getPropertyArray("efak.worknode.disable.cgroup", ",");
            while (itor.hasNext()) {
                ConsumerGroupListing gs = itor.next();
                JSONObject consumerGroup = new JSONObject();
                String groupId = gs.groupId();
                DescribeConsumerGroupsResult descConsumerGroup = adminClient.describeConsumerGroups(Arrays.asList(groupId));
                boolean status = true;
                if (cgroups != null) {
                    for (String cgroup : cgroups) {
                        if (groupId != null) {
                            if (groupId.equals(cgroup)) {
                                status = false;
                                break;
                            }
                        }
                    }
                }
                if (!groupId.contains("efak") && status) {
                    consumerGroup.put("group", groupId);
                    try {
                        Node node = descConsumerGroup.all().get().get(groupId).coordinator();
                        consumerGroup.put("node", node.host() + ":" + node.port());
                    } catch (Exception e) {
                        LOG.error("Get coordinator node has error, msg is " + e.getMessage());
                        e.printStackTrace();
                    }
                    try {
                        if (nodes.size() > 0) {
                            consumerGroup.put("host", nodes.get(nodeIndex).getHost());
                            nodeIndex++;
                            if (nodeIndex == nodes.size() - 1) {
                                // reset index
                                nodeIndex = 0;
                            }
                        }

                    } catch (Exception e) {
                        LOG.error("Get shard node host has error, msg is ", e);
                        e.printStackTrace();
                    }
                    consumerGroup.put("meta", getKafkaMetadata(parseBrokerServer(clusterAlias), groupId, clusterAlias));
                    consumerGroups.add(consumerGroup);
                }
            }
        } catch (Exception e) {
            LOG.error("Get kafka consumer has error,msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return consumerGroups.toString();
    }

    /**
     * Get kafka 0.10.x consumer group & topic information used for page.
     */
    public String getKafkaConsumer(String clusterAlias, DisplayInfo page) {
        Properties prop = new Properties();
        JSONArray consumerGroups = new JSONArray();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            ListConsumerGroupsResult cgrs = adminClient.listConsumerGroups();
            java.util.Iterator<ConsumerGroupListing> itor = cgrs.all().get().iterator();
            if (page.getSearch().length() > 0) {
                int offset = 0;
                boolean flag = itor.hasNext();
                while (flag) {
                    ConsumerGroupListing gs = itor.next();
                    JSONObject consumerGroup = new JSONObject();
                    String groupId = gs.groupId();
                    DescribeConsumerGroupsResult descConsumerGroup = adminClient.describeConsumerGroups(Arrays.asList(groupId));
                    if (!groupId.contains("efak") && groupId.contains(page.getSearch())) {
                        if (offset < (page.getiDisplayLength() + page.getiDisplayStart()) && offset >= page.getiDisplayStart()) {
                            consumerGroup.put("group", groupId);
                            try {
                                Node node = descConsumerGroup.all().get().get(groupId).coordinator();
                                consumerGroup.put("node", node.host() + ":" + node.port());
                            } catch (Exception e) {
                                LOG.error("Get coordinator node has error, msg is " + e.getMessage());
                                e.printStackTrace();
                            }
                            consumerGroup.put("meta", getKafkaMetadata(parseBrokerServer(clusterAlias), groupId, clusterAlias));
                            consumerGroups.add(consumerGroup);
                        }
                        offset++;
                    }
                    flag = itor.hasNext();
                    if (offset >= page.getiDisplayLength() + page.getiDisplayStart()) {
                        flag = false;
                    }
                }
            } else {
                int offset = 0;
                boolean flag = itor.hasNext();
                while (flag) {
                    ConsumerGroupListing gs = itor.next();
                    JSONObject consumerGroup = new JSONObject();
                    String groupId = gs.groupId();
                    DescribeConsumerGroupsResult descConsumerGroup = adminClient.describeConsumerGroups(Arrays.asList(groupId));
                    if (!groupId.contains("efak")) {
                        if (offset < (page.getiDisplayLength() + page.getiDisplayStart()) && offset >= page.getiDisplayStart()) {
                            consumerGroup.put("group", groupId);
                            try {
                                Node node = descConsumerGroup.all().get().get(groupId).coordinator();
                                consumerGroup.put("node", node.host() + ":" + node.port());
                            } catch (Exception e) {
                                LOG.error("Get coordinator node has error, msg is " + e.getMessage());
                                e.printStackTrace();
                            }
                            consumerGroup.put("meta", getKafkaMetadata(parseBrokerServer(clusterAlias), groupId, clusterAlias));
                            consumerGroups.add(consumerGroup);
                        }
                        offset++;
                    }
                    flag = itor.hasNext();
                    if (offset >= page.getiDisplayLength() + page.getiDisplayStart()) {
                        flag = false;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Get kafka consumer has error,msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return consumerGroups.toString();
    }

    /**
     * Get kafka group consumer all topics lags.
     */
    public long getKafkaLag(String clusterAlias, String group, String ketopic) {
        long lag = 0L;

        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            ListConsumerGroupOffsetsResult offsets = adminClient.listConsumerGroupOffsets(group);
            for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.partitionsToOffsetAndMetadata().get().entrySet()) {
                if (ketopic.equals(entry.getKey().topic())) {
                    long logSize = getKafkaLogSize(clusterAlias, entry.getKey().topic(), entry.getKey().partition());
                    lag += logSize - entry.getValue().offset();
                }
            }
        } catch (Exception e) {
            LOG.error("Get cluster[" + clusterAlias + "] group[" + group + "] topic[" + ketopic + "] consumer lag has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return lag;
    }

    /**
     * Get kafka 0.10.x consumer metadata.
     */
    private JSONArray getKafkaMetadata(String bootstrapServers, String group, String clusterAlias) {
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        JSONArray consumerGroups = new JSONArray();
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            DescribeConsumerGroupsResult descConsumerGroup = adminClient.describeConsumerGroups(Arrays.asList(group));
            Collection<MemberDescription> consumerMetaInfos = descConsumerGroup.describedGroups().get(group).get().members();
            Set<String> hasOwnerTopics = new HashSet<>();
            if (consumerMetaInfos.size() > 0) {
                for (MemberDescription consumerMetaInfo : consumerMetaInfos) {
                    JSONObject topicSub = new JSONObject();
                    JSONArray topicSubs = new JSONArray();
                    for (TopicPartition topic : consumerMetaInfo.assignment().topicPartitions()) {
                        JSONObject object = new JSONObject();
                        object.put("topic", topic.topic());
                        object.put("partition", topic.partition());
                        topicSubs.add(object);
                        hasOwnerTopics.add(topic.topic());
                    }
                    topicSub.put("owner", consumerMetaInfo.consumerId());
                    topicSub.put("node", consumerMetaInfo.host().replaceAll("/", ""));
                    topicSub.put("topicSub", topicSubs);
                    consumerGroups.add(topicSub);
                }
            }

            ListConsumerGroupOffsetsResult noActiveTopic = adminClient.listConsumerGroupOffsets(group);
            JSONObject topicSub = new JSONObject();
            JSONArray topicSubs = new JSONArray();
            for (Entry<TopicPartition, OffsetAndMetadata> entry : noActiveTopic.partitionsToOffsetAndMetadata().get().entrySet()) {
                JSONObject object = new JSONObject();
                object.put("topic", entry.getKey().topic());
                object.put("partition", entry.getKey().partition());
                if (!hasOwnerTopics.contains(entry.getKey().topic())) {
                    topicSubs.add(object);
                }
            }
            topicSub.put("owner", "");
            topicSub.put("node", "-");
            topicSub.put("topicSub", topicSubs);
            consumerGroups.add(topicSub);
        } catch (Exception e) {
            LOG.error("Get kafka consumer metadata has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return consumerGroups;
    }

    /**
     * Get kafka 0.10.x consumer pages.
     */
    public String getKafkaActiverSize(String clusterAlias, String group) {
        JSONArray consumerGroups = getKafkaMetadata(parseBrokerServer(clusterAlias), group, clusterAlias);
        int activerCounter = 0;
        Set<String> topics = new HashSet<>();
        for (Object object : consumerGroups) {
            JSONObject consumerGroup = (JSONObject) object;
            if (!"".equals(consumerGroup.getString("owner")) && consumerGroup.getString("owner") != null) {
                activerCounter++;
            }
            for (Object topicObject : consumerGroup.getJSONArray("topicSub")) {
                JSONObject topic = (JSONObject) topicObject;
                topics.add(topic.getString("topic"));
            }
        }
        JSONObject activerAndTopics = new JSONObject();
        activerAndTopics.put("activers", activerCounter);
        activerAndTopics.put("topics", topics.size());
        return activerAndTopics.toString();
    }

    /**
     * Get kafka consumer information pages not owners.
     */
    public OwnerInfo getKafkaActiverNotOwners(String clusterAlias, String group) {
        OwnerInfo ownerInfo = new OwnerInfo();
        JSONArray consumerGroups = getKafkaMetadata(parseBrokerServer(clusterAlias), group, clusterAlias);
        int activerCounter = 0;
        Set<String> topics = new HashSet<>();
        for (Object object : consumerGroups) {
            JSONObject consumerGroup = (JSONObject) object;
            if (!"".equals(consumerGroup.getString("owner")) && consumerGroup.getString("owner") != null) {
                activerCounter++;
            }
            for (Object topicObject : consumerGroup.getJSONArray("topicSub")) {
                JSONObject topic = (JSONObject) topicObject;
                topics.add(topic.getString("topic"));
            }
        }
        ownerInfo.setActiveSize(activerCounter);
        ownerInfo.setTopicSets(topics);
        return ownerInfo;
    }

    /**
     * Get kafka 0.10.x, 1.x, 2.x, 3.x consumer groups.
     */
    public int getKafkaConsumerGroups(String clusterAlias) {
        Properties prop = new Properties();
        int counter = 0;
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }

        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            ListConsumerGroupsResult consumerGroups = adminClient.listConsumerGroups();
            java.util.Iterator<ConsumerGroupListing> groups = consumerGroups.all().get().iterator();
            while (groups.hasNext()) {
                String groupId = groups.next().groupId();
                if (!groupId.contains("efak")) {
                    counter++;
                }
            }
        } catch (Exception e) {
            LOG.info("Get kafka consumer group has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return counter;
    }

    /**
     * Get kafka 0.10.x, 1.x, 2.x consumer topic information.
     */
    public Set<String> getKafkaConsumerTopic(String clusterAlias, String group) {
        JSONArray consumerGroups = getKafkaMetadata(parseBrokerServer(clusterAlias), group, clusterAlias);
        Set<String> topics = new HashSet<>();
        for (Object object : consumerGroups) {
            JSONObject consumerGroup = (JSONObject) object;
            for (Object topicObject : consumerGroup.getJSONArray("topicSub")) {
                JSONObject topic = (JSONObject) topicObject;
                topics.add(topic.getString("topic"));
            }
        }
        return topics;
    }

    /**
     * Get kafka 0.10.x consumer group and topic.
     */
    public String getKafkaConsumerGroupTopic(String clusterAlias, String group) {
        return getKafkaMetadata(parseBrokerServer(clusterAlias), group, clusterAlias).toString();
    }

    /**
     * Get kafka 0.10.x, 1.x, 2.x offset from topic.
     */
    public String getKafkaOffset(String clusterAlias) {
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }
        JSONArray targets = new JSONArray();
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            ListConsumerGroupsResult consumerGroups = adminClient.listConsumerGroups();
            java.util.Iterator<ConsumerGroupListing> groups = consumerGroups.all().get().iterator();
            while (groups.hasNext()) {
                String groupId = groups.next().groupId();
                if (!groupId.contains("efak")) {
                    ListConsumerGroupOffsetsResult offsets = adminClient.listConsumerGroupOffsets(groupId);
                    for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.partitionsToOffsetAndMetadata().get().entrySet()) {
                        JSONObject object = new JSONObject();
                        object.put("group", groupId);
                        object.put("topic", entry.getKey().topic());
                        object.put("partition", entry.getKey().partition());
                        object.put("offset", entry.getValue().offset());
                        object.put("timestamp", CalendarUtils.getDate());
                        targets.add(object);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Get consumer offset has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return targets.toString();
    }

    /**
     * Get the data for the topic partition in the specified consumer group
     */
    public Map<Integer, Long> getKafkaOffset(String clusterAlias, String group, String topic, Set<Integer> partitionids) {
        Map<Integer, Long> partitionOffset = new HashMap<>();
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, parseBrokerServer(clusterAlias));

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(prop, clusterAlias);
        }
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(prop);
            List<TopicPartition> tps = new ArrayList<>();
            for (int partitionid : partitionids) {
                TopicPartition tp = new TopicPartition(topic, partitionid);
                tps.add(tp);
            }

            ListConsumerGroupOffsetsOptions consumerOffsetOptions = new ListConsumerGroupOffsetsOptions();
            consumerOffsetOptions.topicPartitions(tps);

            ListConsumerGroupOffsetsResult offsets = adminClient.listConsumerGroupOffsets(group, consumerOffsetOptions);
            for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.partitionsToOffsetAndMetadata().get().entrySet()) {
                if (topic.equals(entry.getKey().topic())) {
                    partitionOffset.put(entry.getKey().partition(), entry.getValue().offset());
                }
            }
        } catch (Exception e) {
            LOG.error("Get consumer offset has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return partitionOffset;
    }

    /**
     * Get kafka 0.10.x broker bootstrap server.
     */
    public String getKafkaBrokerServer(String clusterAlias) {
        return parseBrokerServer(clusterAlias);
    }

    /**
     * Get kafka 0.10.x topic history logsize.
     */
    public long getKafkaLogSize(String clusterAlias, String topic, int partitionid) {
        long histyLogSize = 0L;
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Kafka.EFAK_SYSTEM_GROUP);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerServer(clusterAlias));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(props, clusterAlias);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, partitionid);
        consumer.assign(Collections.singleton(tp));
        java.util.Map<TopicPartition, Long> logsize = consumer.endOffsets(Collections.singleton(tp));
        try {
            histyLogSize = logsize.get(tp).longValue();
        } catch (Exception e) {
            LOG.error("Get history topic logsize has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return histyLogSize;
    }

    /**
     * Get kafka 0.10.x topic history logsize.
     */
    public Map<TopicPartition, Long> getKafkaLogSize(String clusterAlias, String topic, Set<Integer> partitionids) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Kafka.EFAK_SYSTEM_GROUP);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerServer(clusterAlias));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(props, clusterAlias);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Set<TopicPartition> tps = new HashSet<>();
        for (int partitionid : partitionids) {
            TopicPartition tp = new TopicPartition(topic, partitionid);
            tps.add(tp);
        }

        consumer.assign(tps);
        java.util.Map<TopicPartition, Long> endLogSize = consumer.endOffsets(tps);
        if (consumer != null) {
            consumer.close();
        }
        return endLogSize;
    }

    /**
     * Get kafka 0.10.x topic real logsize by partitionid.
     */
    public long getKafkaRealLogSize(String clusterAlias, String topic, int partitionid) {
        long realLogSize = 0L;
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Kafka.EFAK_SYSTEM_GROUP);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerServer(clusterAlias));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(props, clusterAlias);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(topic, partitionid);
        consumer.assign(Collections.singleton(tp));
        java.util.Map<TopicPartition, Long> endLogSize = consumer.endOffsets(Collections.singleton(tp));
        java.util.Map<TopicPartition, Long> startLogSize = consumer.beginningOffsets(Collections.singleton(tp));
        try {
            realLogSize = endLogSize.get(tp).longValue() - startLogSize.get(tp).longValue();
        } catch (Exception e) {
            LOG.error("Get real topic logsize by partition list has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return realLogSize;
    }

    /**
     * Get kafka 0.10.x topic real logsize by partitionid set.
     */
    public long getKafkaRealLogSize(String clusterAlias, String topic, Set<Integer> partitionids) {
        long realLogSize = 0L;
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Kafka.EFAK_SYSTEM_GROUP);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerServer(clusterAlias));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(props, clusterAlias);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Set<TopicPartition> tps = new HashSet<>();
        for (int partitionid : partitionids) {
            TopicPartition tp = new TopicPartition(topic, partitionid);
            tps.add(tp);
        }
        consumer.assign(tps);
        java.util.Map<TopicPartition, Long> endLogSize = consumer.endOffsets(tps);
        java.util.Map<TopicPartition, Long> startLogSize = consumer.beginningOffsets(tps);
        try {
            long endSumLogSize = 0L;
            long startSumLogSize = 0L;
            for (Entry<TopicPartition, Long> entry : endLogSize.entrySet()) {
                endSumLogSize += entry.getValue();
            }
            for (Entry<TopicPartition, Long> entry : startLogSize.entrySet()) {
                startSumLogSize += entry.getValue();
            }
            realLogSize = endSumLogSize - startSumLogSize;
        } catch (Exception e) {
            LOG.error("Get real topic logsize has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return realLogSize;
    }

    /**
     * Get topic producer send logsize records.
     */
    public long getKafkaProducerLogSize(String clusterAlias, String topic, Set<Integer> partitionids) {
        long producerLogSize = 0L;
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Kafka.EFAK_SYSTEM_GROUP);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerServer(clusterAlias));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(props, clusterAlias);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Set<TopicPartition> tps = new HashSet<>();
        for (int partitionid : partitionids) {
            TopicPartition tp = new TopicPartition(topic, partitionid);
            tps.add(tp);
        }
        consumer.assign(tps);
        java.util.Map<TopicPartition, Long> endLogSize = consumer.endOffsets(tps);
        try {
            for (Entry<TopicPartition, Long> entry : endLogSize.entrySet()) {
                producerLogSize += entry.getValue();
            }
        } catch (Exception e) {
            LOG.error("Get producer topic logsize has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return producerLogSize;
    }

    /**
     * Get kafka version.
     */
    public String getKafkaVersion(String host, int port, String ids, String clusterAlias) {
        JMXConnector connector = null;
        String version = "-";
        String JMX = SystemConfigUtils.getProperty(clusterAlias + ".efak.jmx.uri");
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(JMX, host + ":" + port));
            connector = JMXFactoryUtils.connectWithTimeout(clusterAlias, jmxSeriverUrl, 30, TimeUnit.SECONDS);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            if (CollectorType.KAFKA.equals(SystemConfigUtils.getProperty(clusterAlias + ".efak.offset.storage"))) {
                version = mbeanConnection.getAttribute(new ObjectName(String.format(BrokerServer.BROKER_VERSION.getValue(), ids)), BrokerServer.BROKER_VERSION_VALUE.getValue()).toString();
            } else {
                version = mbeanConnection.getAttribute(new ObjectName(KafkaServer8.VERSION.getValue()), KafkaServer8.VALUE.getValue()).toString();
            }
        } catch (Exception ex) {
            LOG.error("Get kafka version from jmx has error, msg is " + ex.getMessage());
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (IOException e) {
                    LOG.error("Close jmx connector has error, msg is " + e.getMessage());
                }
            }
        }
        return version;
    }

    /**
     * Get kafka 0.10.x sasl topic metadata from kafka.
     */
    @Override
    public List<MetadataInfo> findKafkaLeader(String clusterAlias, String topic) {
//        List<MetadataInfo> targets = new ArrayList<>();
//        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
//        try {
//            if (zkc.pathExists(BROKER_TOPICS_PATH + "/" + topic)) {
//                Tuple2<Option<byte[]>, Stat> tuple = zkc.getDataAndStat(BROKER_TOPICS_PATH + "/" + topic);
//                String tupleString = new String(tuple._1.get());
//                JSONObject partitionObject = JSON.parseObject(tupleString).getJSONObject("partitions");
//                for (String partition : partitionObject.keySet()) {
//                    String path = String.format(TOPIC_ISR, topic, Integer.valueOf(partition));
//                    Tuple2<Option<byte[]>, Stat> tuple2 = zkc.getDataAndStat(path);
//                    String tupleString2 = new String(tuple2._1.get());
//                    JSONObject topicMetadata = JSON.parseObject(tupleString2);
//                    MetadataInfo metadate = new MetadataInfo();
//                    try {
//                        metadate.setLeader(topicMetadata.getInteger("leader"));
//                    } catch (Exception e) {
//                        LOG.error("Parse string brokerid to int has error, brokerid[" + topicMetadata.getString("leader") + "]");
//                        e.printStackTrace();
//                    }
//                    metadate.setPartitionId(Integer.valueOf(partition));
//                    targets.add(metadate);
//                }
//            }
//        } catch (Exception e) {
//            LoggerUtils.print(this.getClass()).error("Find kafka partition leader has error, msg is ", e);
//        } finally {
//            if (zkc != null) {
//                kafkaZKPool.release(clusterAlias, zkc);
//                zkc = null;
//            }
//        }
        KafkaSchemaFactory ksf = new KafkaSchemaFactory(new KafkaStoragePlugin());
        return ksf.getTopicPartitionsLeader(clusterAlias, topic);
    }

    /**
     * Send mock message to kafka topic .
     */
    public boolean mockMessage(String clusterAlias, String topic, String message) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerServer(clusterAlias));
        props.put(Kafka.KEY_SERIALIZER, StringSerializer.class.getCanonicalName());
        props.put(Kafka.VALUE_SERIALIZER, StringSerializer.class.getCanonicalName());
        props.put(Kafka.PARTITION_CLASS, KafkaPartitioner.class.getName());

        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
            ssl(props, clusterAlias);
        }
        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, String>(topic, new Date().getTime() + "", message));
        producer.close();

        return true;
    }

    /**
     * Get broker host and jmx_port info from ids.
     */
    public String getBrokerJMXFromIds(String clusterAlias, int ids) {
//        String jni = "";
//        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
//        try {
//            if (zkc.pathExists(BROKER_IDS_PATH)) {
//                try {
//                    Tuple2<Option<byte[]>, Stat> tuple = zkc.getDataAndStat(BROKER_IDS_PATH + "/" + ids);
//                    String tupleString = new String(tuple._1.get());
//                    String host = "";
//                    if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.sasl.enable") || SystemConfigUtils.getBooleanProperty(clusterAlias + ".efak.ssl.enable")) {
//                        String endpoints = JSON.parseObject(tupleString).getString("endpoints");
//                        String tmp = endpoints.split("//")[1];
//                        host = tmp.substring(0, tmp.length() - 2).split(":")[0];
//                    } else {
//                        host = JSON.parseObject(tupleString).getString("host");
//                    }
//                    int jmxPort = JSON.parseObject(tupleString).getInteger("jmx_port");
//                    jni = host + ":" + jmxPort;
//                } catch (Exception ex) {
//                    LOG.error("Get broker from ids has error, msg is " + ex.getCause().getMessage());
//                    ex.printStackTrace();
//                }
//            }
//        } catch (Exception e) {
//            LoggerUtils.print(this.getClass()).error("Get broker jmx info from ids has error,msg is ", e);
//        } finally {
//            if (zkc != null) {
//                kafkaZKPool.release(clusterAlias, zkc);
//                zkc = null;
//            }
//        }
        KafkaSchemaFactory ksf = new KafkaSchemaFactory(new KafkaStoragePlugin());
        return ksf.getBrokerJMXFromIds(clusterAlias, ids);
    }

    /**
     * Get kafka os memory.
     */
    public long getOSMemory(String clusterAlias, String host, int port, String property) {
        JMXConnector connector = null;
        long memory = 0L;
        String JMX = SystemConfigUtils.getProperty(clusterAlias + ".efak.jmx.uri");
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(JMX, host + ":" + port));
            connector = JMXFactoryUtils.connectWithTimeout(clusterAlias, jmxSeriverUrl, 30, TimeUnit.SECONDS);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            MemoryMXBean memBean = ManagementFactory.newPlatformMXBeanProxy(mbeanConnection, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            long max = memBean.getHeapMemoryUsage().getMax();
            long used = memBean.getHeapMemoryUsage().getUsed();
            if (BrokerServer.TOTAL_PHYSICAL_MEMORY_SIZE.getValue().equals(property)) {
                memory = max;
            } else if (BrokerServer.FREE_PHYSICAL_MEMORY_SIZE.getValue().equals(property)) {
                memory = max - used;
            }
        } catch (Exception ex) {
            LOG.error("Get kafka os memory from jmx has error, msg is " + ex.getMessage());
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (IOException e) {
                    LOG.error("Close kafka os memory jmx connector has error, msg is " + e.getMessage());
                }
            }
        }
        return memory;
    }

    /**
     * Get kafka cpu.
     */
    public String getUsedCpu(String clusterAlias, String host, int port) {
        JMXConnector connector = null;
        String JMX = SystemConfigUtils.getProperty(clusterAlias + ".efak.jmx.uri");
        String cpu = "<span class='badge bg-light-danger text-danger'>NULL</span>";
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(JMX, host + ":" + port));
            connector = JMXFactoryUtils.connectWithTimeout(clusterAlias, jmxSeriverUrl, 30, TimeUnit.SECONDS);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            String value = mbeanConnection.getAttribute(new ObjectName(BrokerServer.JMX_PERFORMANCE_TYPE.getValue()), BrokerServer.PROCESS_CPU_LOAD.getValue()).toString();
            double cpuValue = Double.parseDouble(value);
            String percent = StrUtils.numberic((cpuValue * 100.0) + "") + "%";
            if ((cpuValue * 100.0) < BrokerSever.CPU_NORMAL) {
                cpu = "<span class='badge bg-light-success text-success'>" + percent + "</span>";
            } else if ((cpuValue * 100.0) >= BrokerSever.CPU_NORMAL && (cpuValue * 100.0) < BrokerSever.CPU_DANGER) {
                cpu = "<span class='badge bg-light-warning text-warning'>" + percent + "</span>";
            } else if ((cpuValue * 100.0) >= BrokerSever.CPU_DANGER) {
                cpu = "<span class='badge bg-light-danger text-danger'>" + percent + "</span>";
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Get kafka broker used cpu has error, msg is ", e);
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (IOException e) {
                    LOG.error("Close kafka broker cpu jmx connector has error, msg is " + e.getMessage());
                }
            }
        }
        return cpu;
    }

    /**
     * Get kafka cpu value.
     */
    public double getUsedCpuValue(String clusterAlias, String host, int port) {
        JMXConnector connector = null;
        String JMX = SystemConfigUtils.getProperty(clusterAlias + ".efak.jmx.uri");
        double cpu = 0.00;
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(JMX, host + ":" + port));
            connector = JMXFactoryUtils.connectWithTimeout(clusterAlias, jmxSeriverUrl, 30, TimeUnit.SECONDS);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            String value = mbeanConnection.getAttribute(new ObjectName(BrokerServer.JMX_PERFORMANCE_TYPE.getValue()), BrokerServer.PROCESS_CPU_LOAD.getValue()).toString();
            double cpuValue = Double.parseDouble(value);
            cpu = StrUtils.numberic(String.valueOf(cpuValue * 100.0));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Get kafka broker used cpu value has error, msg is ", e);
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (IOException e) {
                    LOG.error("Close kafka used cpu value jmx connector has error, msg is " + e.getMessage());
                }
            }
        }
        return cpu;
    }

    /**
     * Get kafka used memory.
     */
    public String getUsedMemory(String clusterAlias, String host, int port) {
        JMXConnector connector = null;
        String JMX = SystemConfigUtils.getProperty(clusterAlias + ".efak.jmx.uri");
        String memory = "<span class='badge bg-light-danger text-danger'>NULL</span>";
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(JMX, host + ":" + port));
            connector = JMXFactoryUtils.connectWithTimeout(clusterAlias, jmxSeriverUrl, 30, TimeUnit.SECONDS);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            MemoryMXBean memBean = ManagementFactory.newPlatformMXBeanProxy(mbeanConnection, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            long used = memBean.getHeapMemoryUsage().getUsed();
            long max = memBean.getHeapMemoryUsage().getMax();
            String percent = StrUtils.stringify(used) + " (" + StrUtils.numberic((used * 100.0 / max) + "") + "%)";
            if ((used * 100.0) / max < BrokerSever.MEM_NORMAL) {
                memory = "<span class='badge bg-light-success text-success'>" + percent + "</span>";
            } else if ((used * 100.0) / max >= BrokerSever.MEM_NORMAL && (used * 100.0) / max < BrokerSever.MEM_DANGER) {
                memory = "<span class='badge bg-light-warning text-warning'>" + percent + "</span>";
            } else if ((used * 100.0) / max >= BrokerSever.MEM_DANGER) {
                memory = "<span class='badge badge-danger text-danger'>" + percent + "</span>";
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Get kafka broker used memroy has error, msg is ", e);
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (IOException e) {
                    LOG.error("Close kafka used memory jmx connector has error, msg is " + e.getMessage());
                }
            }
        }
        return memory;
    }

}

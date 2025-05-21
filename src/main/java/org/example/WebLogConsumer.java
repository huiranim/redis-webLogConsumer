package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class WebLogConsumer {
    private static final String BOOTSTRAP_SERVERS = "192.168.150.115:9192,192.168.150.115:9194,192.168.150.125:9192";
    private static final String GROUP_ID = "web-log-consumer-group";
    private static final String TOPIC = "hr-test-web-log";
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // ISO 형식으로 출력

    public static void main(String[] args) throws SQLException {
        // Kafka Consumer 설정
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC));

        // DB 연결 (최종 저장용)
        Connection conn;
        try {
            String jdbcUrl = "jdbc:mysql://192.168.150.100:3306/rwkcp";
            String dbUser = "rwkcp_app";
            String dbPass = "fpemdnemzpdl123$";
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
        } catch (SQLException e) {
            System.err.println("JDBC 연결 실패: " + e.getMessage());
            return;
        }

        // Redis Cluster 연결(참조용)
        JedisCluster jedis = null;
        try {
            Set<HostAndPort> clusterNodes = Set.of(
                    new HostAndPort("192.168.150.115", 7002),
                    new HostAndPort("192.168.150.120", 7002),
                    new HostAndPort("192.168.150.125", 7002),
                    new HostAndPort("192.168.150.115", 7003),
                    new HostAndPort("192.168.150.120", 7003),
                    new HostAndPort("192.168.150.125", 7003)
            );
            jedis = new JedisCluster(clusterNodes);
        } catch (Exception e) {
            System.err.println("RedisCluster 연결 실패: " + e.getMessage());
            return;
        }

        System.out.println("Listening for messages...");

        // Throughput 측정
        AtomicLong totalProcessed = new AtomicLong(); // 누적 메시지 수
        long startTime = System.currentTimeMillis();

        // 메시지 컨슈밍
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
//                System.out.println("Consumed message: "+record.value());

                try {
                    Map<String, Object> data = mapper.readValue(record.value(), Map.class);

                    // ISO 8601 형식의 timestamp 처리
                    String timestampStr = (String) data.get("timestamp");
                    Instant instant = Instant.parse(timestampStr);  // ISO 8601 형식 파싱
                    Timestamp timestamp = Timestamp.from(instant);  // Timestamp로 변환

                    String cusno = (String) data.get("cusno");
                    String url = (String) data.get("url");
                    String httpMethod = (String) data.get("http_method");
                    Integer responseTime = (Integer) data.get("response_time");
                    String ipAddress = (String) data.get("ip_address");
                    Integer statusCode = (Integer) data.get("status_code");
                    String serviceId = (String) data.get("service_id");
                    String prodCd = (String) data.get("prod_cd");

                    // Redis 데이터 조회
                    String cusGrade = jedis.hget(cusno, "CUS_GRADE");

                    // VVIP 고객만 감지
                    if ("01".equalsIgnoreCase(cusGrade)) {
//                            System.out.println("VVIP Detected: " + cusno);

                        // DB 저장
                        String sql = "INSERT INTO TB_HR_TEST_WEB_LOG (timestamp, cusno, url, http_method, response_time, ip_address, status_code, service_id, prod_cd) " +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
//                            System.out.println("PreparedStatement sql: "+sql);

                        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                            stmt.setTimestamp(1, timestamp);
                            stmt.setString(2, cusno);
                            stmt.setString(3, url);
                            stmt.setString(4, httpMethod);
                            stmt.setInt(5, responseTime);
                            stmt.setString(6, ipAddress);
                            stmt.setInt(7, statusCode);
                            stmt.setString(8, serviceId);
                            stmt.setString(9, prodCd);

                            stmt.executeUpdate();

                        } catch (SQLException e) {
                            System.err.println("Failed to save record to DB: " + e.getMessage());
                        }
                    }

                    // 메시지 1건 처리 후 count 증가
                    totalProcessed.incrementAndGet();

                } catch (Exception e) {
                    System.err.println("Error processing Kafka record or Redis data: " + e.getMessage());
                }
            }

            long now = System.currentTimeMillis();
            if (now - startTime >= 60000) { // 60초 후 처리량 출력
                long count = totalProcessed.getAndSet(0); // count 초기화
                System.out.println("Throughput: " + count + " records/min");
                startTime = now;
            }
        }
    }
}
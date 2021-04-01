package v2.com.nyble;

import com.nyble.util.DBUtil;
import com.nyble.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import v2.com.nyble.dataStructures.ConsumerFeatures;
import v2.com.nyble.dataStructures.ConsumerMapping;
import v2.com.nyble.manager.RuleManager;
import v2.com.nyble.manager.RulesManager;
import v2.com.nyble.rules.FullNameEmailRule;
import v2.com.nyble.rules.FullNamePhoneRule;
import v2.com.nyble.rules.PhoneEmailRule;

import java.io.*;
import java.sql.*;
import java.util.*;

@Component
public class StartUpRunner  {
    //implements CommandLineRunner

    private static Logger logger = LoggerFactory.getLogger(StartUpRunner.class);

    @Scheduled(fixedDelay = 3600000)
    public void run() throws Exception{
        ConsumerMapping consumerMapping = createMapping();
        logger.info("end creating map");
        RulesManager rulesManager = new RulesManager(consumerMapping);
        rulesManager.registerRuleManager(new RuleManager(new FullNamePhoneRule()));
        rulesManager.registerRuleManager(new RuleManager(new FullNameEmailRule()));
        rulesManager.registerRuleManager(new RuleManager(new PhoneEmailRule()));

        processConsumers(rulesManager);
        logger.info("end creating buckets");

        Collection<Set<String>> groups = rulesManager.getBuckets();
        List<Pair<String, String>> consumersToUpdate = getChangedGroups(groups);
        final String now = System.currentTimeMillis()+"";
        consumersToUpdate.parallelStream().forEach(consumer->{
            String key = consumer.rightSide;
            String[] tokens = consumer.leftSide.split("#");
            int systemId = Integer.parseInt(tokens[0]);
            int consumerId = Integer.parseInt(tokens[1]);
            Utils.updateConsumerAttribute(systemId, consumerId, "entityId", key, now);
        });

        logger.info("End consumer matcher");
    }

    private List<Pair<String, String>> getChangedGroups(Collection<Set<String>> groups) throws IOException, InterruptedException, SQLException {
        List<Pair<String,String>> consumerAndGroup = new ArrayList<>();
        logger.info("Creating consumer groups file");
        final String filePath = createConsumerGroupsFile(groups);
        final String dataSource = "datawarehouse";
        final String query = "select t.system_id, t.consumer_id, t.group_key from tmp_consumer_dedupe_groups t\n" +
                "left join consumers_unique_entity_criterias cuec \n" +
                "on t.system_id = cuec.system_id and t.consumer_id = cuec.consumer_id and t.group_key = cuec.entity_id \n" +
                "where cuec.consumer_id is null";
        logger.info("Loading consumer groups file to DB");
        loadFileToDb(filePath, dataSource);
        if(! new File(filePath).delete()){
            logger.error("Could not delete consumer groups file: {}", filePath);
        }
        logger.info("Resolving diffs...");
        try(Connection conn = DBUtil.getInstance().getConnection(dataSource);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){
            while(rs.next()){
                int systemId = rs.getInt("system_id");
                int consumerId = rs.getInt("consumer_id");
                String groupKey = rs.getString("group_key");
                consumerAndGroup.add(new Pair<>(systemId+"#"+consumerId, groupKey));
            }
        }
        logger.info("Found {} consumers that changed their group", consumerAndGroup.size());
        return consumerAndGroup;
    }

    private String createConsumerGroupsFile(Collection<Set<String>> groups) throws IOException {
        final String fileName = "consumer_groups_"+System.currentTimeMillis()+".csv";
        try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/"+fileName)))){
            groups.forEach(group->{
                String uuid = UUID.randomUUID().toString();
                String groupHash = Utils.toSHA1(String.join(",", group));
                for(String consumerIdentification : group){
                    String[] tokens = consumerIdentification.split("#");
                    int systemId = Integer.parseInt(tokens[0]);
                    int consumerId = Integer.parseInt(tokens[1]);
                    try {
                        bw.write(systemId+","+consumerId+","+groupHash+"\n");
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }

                if(group.size() > 1){
                    logger.debug(uuid+": "+group);
                }
            });
        }
        return "/tmp/"+fileName;
    }

    private void loadFileToDb(String filePath, String dataSourceName) throws IOException, InterruptedException {
        String dbUsername = DBUtil.getInstance().getDataSourceProperties(dataSourceName).get("USERNAME");
        String dbIp = DBUtil.getInstance().getDataSourceProperties(dataSourceName).get("IP");
        String dbDatabase = DBUtil.getInstance().getDataSourceProperties(dataSourceName).get("DATABASE");

        Utils.runSqlFromTerminal(dataSourceName,
                String.format("psql -U %s -h %s -d %s -c \"drop table if exists tmp_consumer_dedupe_groups;\"",
                        dbUsername, dbIp, dbDatabase));
        Utils.runSqlFromTerminal(dataSourceName,
                String.format("psql -U %s -h %s -d %s -c \"create table tmp_consumer_dedupe_groups(" +
                                "system_id integer, consumer_id integer, group_key text);\"",
                        dbUsername, dbIp, dbDatabase));
        Utils.runSqlFromTerminal(dataSourceName,
                String.format("psql -U %s -h %s -d %s -c \"\\copy tmp_consumer_dedupe_groups from '%s' csv delimiter ','\"",
                        dbUsername, dbIp, dbDatabase, filePath));
    }

    private ConsumerMapping createMapping() throws SQLException {
        ConsumerMapping cm = new ConsumerMapping();
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select system_id||'#'||consumer_id from consumers ")){
            while(rs.next()){
                cm.mapConsumer(rs.getString(1));
                if(cm.getSize()%10000 == 0){
                    logger.debug("Mapped {} consumers", cm.getSize());
                }
            }
        }
        return cm;
    }

    private void processConsumers(RulesManager rulesManager) throws SQLException {
        final String query = "select cuec.system_id, cuec.consumer_id,\n" +
                "\tcuec.full_name as \"fullName\", cuec.phone, cuec.email, cf.is_phone_valid::text as \"phoneConfirmed\", " +
                "cf.is_email_valid::text as \"emailConfirmed\"\n" +
                "from consumers_unique_entity_criterias cuec join consumers_flags cf \n" +
                "on cuec.system_id = cf.system_id and cuec.consumer_id = cf.consumer_id ";
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)){
            while(rs.next()){
                int systemId = rs.getInt("system_id");
                int consumerId = rs.getInt("consumer_id");
                String fullName = rs.getString("fullName");
                String phone = rs.getString("phone");
                String email = rs.getString("email");
                String phoneConfirmed = rs.getString("phoneConfirmed");
                String emailConfirmed = rs.getString("emailConfirmed");
                ConsumerFeatures cf = new ConsumerFeatures(systemId, consumerId);
                cf.addFeature("fullName", fullName);
                cf.addFeature("phone", phone);
                cf.addFeature("email", email);
                cf.addFeature("phoneConfirmed", phoneConfirmed);
                cf.addFeature("emailConfirmed", emailConfirmed);
                rulesManager.updateGroups(cf);
            }
        }
    }
}

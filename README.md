# ConsumerMatcher
Kafka project for matching multiple consumer accounts under the same consumer entity

## Embedded WebServer
Running port is 7006

## Redeploy

Kill ConsumerMatcher
```shell script
ps -ef | grep "[C]onsumerMatcher" | grep -v grep | awk '{print $2}' | xargs kill
```
Start new instance of ConsumerMatcher:
```shell script
cd /home/crmsudo/jobs/kafkaClients/ && ./scripts/startLatestBuild.sh ConsumerMatcher.jar
```

## Reload attributes if necessary:

Check if ConsumerAttributesSinkToTable has lag 0 and kill it:
```shell script
ps -ef | grep "[C]onsumerAttributes" | grep -v grep | awk '{print $2}' | xargs kill
```

Check if LoadConsumerUniqueAttributes is running:
```shell script
ps -ef | grep "[L]oadConsumerUniqueAttributes" | grep -v grep | awk '{print $2}' | xargs kill
```

Remove unique entity attributes from consumers table:
```sql
update consumers 
set payload = payload - 'fullName' - 'location' - 'phone' - 'email' - 'birthDate' -'entityId';
```

Empty unique entity tables
```sql
delete from consumers_unique_entity_criterias;
update config_parameters set value='2014-01-03 00:00:00' 
where key='POLL_CONSUMER_ATTRS_LAST_RUN_RMC';
update config_parameters set value='2019-01-23 00:00:00' 
where key='POLL_CONSUMER_ATTRS_LAST_RUN_RRP';
```

Start ConsumerAttributesSinkToTable jar:
```shell script
cd /home/crmsudo/jobs/kafkaClients/ && ./scripts/startLatestBuild.sh ConsumerAttributesSinkToTable.jar
```

Start LoadConsumerUniqueAttributes jar:
```shell script
cd /home/crmsudo/jobs/kafkaClients/ && ./scripts/startLatestBuild.sh LoadConsumerUniqueAttributes.jar
```


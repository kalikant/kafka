kafka-topics.sh --zookeeper zookeeperhost:2181 --create --topic credit_card --partitions 5 --replication-factor 1

kafka-topics.sh --list --zookeeper zookeeperhost:2181

kafka-topics.sh --describe --zookeeper zookeeperhost:2181 --topic credit_card

kafka-console-producer.sh --broker-list zookeeperhost:2181 --topic credit_card

kafka-topics.sh --zookeeper zookeeperhost:2181 --delete --topic testrep (this will just mark for deletion)

kafka-topics.sh --zookeeper zookeeperhost:2181 --alter --topic credit_card --partitions 1 (can only increase the partitions)

(WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected)

kafka-console-consumer.sh --zookeeper zookeeperhost:2181 --topic credit_card --from-beginning

kafka-consumer-groups.sh --zookeeper zookeeperhost:2181 --describe --topic credit_card

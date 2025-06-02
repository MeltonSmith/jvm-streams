BUILD_TIME?=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
BOOTSTRAP_SERVERS=eprupetw10aa:30903,eprupetw10aa:30448,eprupetw10aa:30805

.PHONY: Creating topics needed for app
topics_create:
	@echo "creating topic test-topic"
	kafka-topics.sh --bootstrap-server eprupetw10aa:30903 --topic test-topic --partitions 3 --replication-factor 2 \
	\ --config cleanup.policy=compact --create

topics_copier_create:
	kafka-topics.sh --bootstrap-server eprupetw10aa:30903 --topic output-copier-topic --partitions 3 --replication-factor 2 \
	\ --config cleanup.policy=compact --create

.PHONY: Test topic describe
topics_test_describe:
	kafka-topics.sh --bootstrap-server eprupetw10aa:30903 --topic test-topic --describe

.PHONY: Describe consumer group for copier
describe_group:
	kafka-consumer-groups.sh --bootstrap-server eprupetw10aa:30903,eprupetw10aa:30448, eprupetw10aa:30805 --describe --group tx_copier_group

transfers_topic_create:
	@echo "creating transfers_topic"
	kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVERS} --topic transfers_topic --partitions 3 --replication-factor 2 \
	\ --config cleanup.policy=compact --create
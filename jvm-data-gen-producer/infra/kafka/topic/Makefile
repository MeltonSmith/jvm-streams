

.PHONY: Creating topics needed for app
topics create:
	kafka-topics.sh --bootstrap-server eprupetw10aa:30903 --topic test-topic --partitions 3 --replication-factor 2 \
	\ --config cleanup.policy=compact --create

topics describe:
	kafka-topics.sh --bootstrap-server eprupetw10aa:30903 --topic test-topic --describe
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers = 'localhost:9092', client_id = 'realtime_voting_system')
topic_list = []
voters_topic = NewTopic(name = 'voters_topic', num_partitions = 1, replication_factor = 1)
votes_topic = NewTopic(name = 'votes_topic', num_partitions = 1, replication_factor = 1)
vpc_topic = NewTopic(name = 'aggregated_votes_per_candidate', num_partitions = 1, replication_factor = 1)
tbl_topic = NewTopic(name = 'aggregated_turnout_by_location', num_partitions = 1, replication_factor = 1)
topic_list.append(voters_topic)
topic_list.append(votes_topic)
topic_list.append(vpc_topic)
topic_list.append(tbl_topic)
admin_client.create_topics(new_topics = topic_list)
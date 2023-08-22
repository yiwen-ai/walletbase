CREATE KEYSPACE IF NOT EXISTS walletbase_test
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE walletbase_test;

CREATE KEYSPACE IF NOT EXISTS walletbase
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };
USE walletbase;
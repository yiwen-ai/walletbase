CREATE KEYSPACE IF NOT EXISTS walletbase
WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '3' };
USE walletbase;
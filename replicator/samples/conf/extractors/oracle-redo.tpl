# Oracle redo log extractor via Plog files. 
replicator.extractor.dbms=com.continuent.tungsten.replicator.extractor.oracle.redo.PlogExtractor

# Location of directory containing plog files. 
replicator.extractor.dbms.plogDirectory=@{REPL_ORACLE_REDO_MINER_DIRECTORY}/mine

# The fragment size for transactions. A value of 0 means that transactions
# will not be fragmented. 
replicator.extractor.dbms.transactionFragSize=@{REPL_ORACLE_REDO_MINER_TRANSACTION_FRAGMENT_SIZE}

# The extractor queue size, which is the maximum number of rows that 
# plog extraction can enqueue before pausing to wait for processing. 
replicator.extractor.dbms.queueSize=100

# The number of milliseconds to pause if no new data is in the current plog
# file. 
replicator.extractor.dbms.sleepSizeInMilliseconds=1000

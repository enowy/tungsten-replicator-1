# Oracle redo log extractor via Plog files. 
replicator.extractor.dbms=com.continuent.tungsten.replicator.extractor.oracle.redo.PlogExtractor

# Location of directory containing plog files. 
replicator.extractor.dbms.plogDirectory=@{REPL_ORACLE_REDO_MINER_DIRECTORY}/mine

# The fragment size for transactions. A value of 0 means that transactions
# will not be fragmented.  A positive integer indicates the number of row 
# changes to include in a single transaction. 
replicator.extractor.dbms.transactionFragSize=@{REPL_ORACLE_REDO_MINER_TRANSACTION_FRAGMENT_SIZE}

# Number of row changes per transaction to hold as Java objects.  Above this
# number transactions spill to byte array cache in storage.  The value should
# be large enough to ensure that all normally occurring OLTP transactions
# remain as Java objects.  
replicator.extractor.dbms.lcrBufferLimit=10000

# Location of cache used to buffer large Oracle transactions while waiting for
# a commit in order to serialize. The required storage space is potentially 
# in the range of multiple gigabytes
replicator.extractor.dbms.cacheDir=@{HOME_DIRECTORY}/tmp/@{SERVICE.DEPLOYMENT_SERVICE}/bytecache

# The extractor queue size, which is the maximum number of rows that 
# plog extraction can enqueue before pausing to wait for processing. 
replicator.extractor.dbms.queueSize=100

# The number of milliseconds to pause if no new data is in the current plog
# file. 
replicator.extractor.dbms.sleepSizeInMilliseconds=500

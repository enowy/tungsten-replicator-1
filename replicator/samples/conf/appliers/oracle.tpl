// Oracle applier configuration. 
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.OracleApplier
replicator.applier.dbms.dataSource=global
replicator.applier.dbms.getColumnMetadataFromDB=false
@{#(APPLIER.REPL_SVC_DATASOURCE_APPLIER_INIT_SCRIPT)}replicator.applier.dbms.initScript=@{APPLIER.REPL_SVC_DATASOURCE_APPLIER_INIT_SCRIPT}

# If true enable JDBC batching.  The second parameter sets the maximum batch
# size. 
replicator.applier.dbms.optimizeRowEvents=@{REPL_SVC_APPLIER_OPTIMIZE_ROW_EVENTS}
replicator.applier.dbms.maxRowBatchSize=20

# Required to enable tests to work. (Do not remove.)
replicator.applier.oracle.service=@{APPLIER.REPL_ORACLE_SERVICE}
replicator.applier.oracle.sid=@{APPLIER.REPL_ORACLE_SID}

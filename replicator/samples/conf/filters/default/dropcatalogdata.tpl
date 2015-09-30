# Filter to drop data from the replicator catalog.  This needs to be applied
# immediately after extraction for SQL data sources that cannot suppress 
# logging of Tungsten catalog metadata DDL and DML. 
replicator.filter.dropcatalogdata=com.continuent.tungsten.replicator.filter.CatalogDataFilter

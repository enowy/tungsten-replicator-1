-- VMware Continuent Tungsten Replicator
-- Copyright (C) 2015 VMware, Inc. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
--      
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- Pre Requisite Check Script for v5 Continuent installs
-- Author: Chris Parker
-- Version: 1.2

CREATE OR REPLACE PACKAGE ContinuentPreReqCheck AS

	PROCEDURE getVersion (
		nVersionOK	OUT NUMBER, 
		nMajor 		OUT NUMBER, 
		nMinor 		OUT NUMBER, 
		vLogFile 	IN  VARCHAR2 );
	
	PROCEDURE checkDataTypes (
		nDataTypeOK	OUT NUMBER, 
		vTarget 	IN  VARCHAR2, 
		vOwners 	IN  VARCHAR2, 
		nMajor 		IN  NUMBER, 
		nMinor 		IN  NUMBER, 
		vLogFile 	IN  VARCHAR2 );
	
	PROCEDURE checkPKExists (
		nPKCheckOK  OUT NUMBER ,
		vOwners     IN  VARCHAR2,
		vLogFile	IN  VARCHAR2 );

	PROCEDURE checkFeatures (
		nFeaturesOK OUT NUMBER ,
		vOwners     IN  VARCHAR2,
		vLogFile	IN  VARCHAR2 );
					
	PROCEDURE checkUser (
		nUserOK		OUT NUMBER,
		vReplUser	IN	VARCHAR2,
		vLogFile	IN  VARCHAR2 );

	PROCEDURE run (
		vTarget   IN VARCHAR2, 
		vOwners   IN VARCHAR2,
		vReplUser IN VARCHAR2,
		vLogDir   IN VARCHAR2 );
  
  	PROCEDURE logResult (
		vLogFile 	IN VARCHAR2, 
		vLogString 	IN VARCHAR2 );
	
	PROCEDURE configLogDir (
		vLogDir		IN VARCHAR2 );
		
END ContinuentPreReqCheck;
/


CREATE OR REPLACE PACKAGE BODY ContinuentPreReqCheck AS

	PROCEDURE getVersion (
		nVersionOK OUT NUMBER ,
		nMajor     OUT NUMBER ,
		nMinor     OUT NUMBER ,
		vLogFile   IN  VARCHAR2 ) IS
	BEGIN
		/* Simply just grab the version of the DB, make sure it's 9.2 or greater */
		
		SELECT SUBSTR(version, 1 ,INSTR(version,'.')-1), 
			SUBSTR(version, (INSTR(version,'.') + 1), (INSTR(version,'.',1,2) - (INSTR(version,'.')+1))) 
			INTO nMajor, nMinor 
		FROM v$instance;

	  	CASE 
	    	WHEN (nMajor IN (10,11,12) OR (nMajor IN (9) AND nMinor > 1)) THEN nVersionOK := 1;
			ELSE nVersionOK := 0;
		END CASE;
	  	
		logResult(vLogFile, 'INFO  >> Major Version : '||nMajor||' Minor Version : '||nMinor);
		
	END getVersion;

	PROCEDURE checkDataTypes (
		nDataTypeOK OUT NUMBER ,
		vTarget		IN  VARCHAR2, /* ORACLE, MYSQL55, MYSQL56, MYSQL57, MSSQL */
		vOwners     IN  VARCHAR2,
		nMajor      IN  NUMBER ,
		nMinor      IN  NUMBER ,
		vLogFile	IN  VARCHAR2 ) IS
		vSQL			VARCHAR2(32767);
		vWhere			VARCHAR2(2000);
		vOwnerString 	VARCHAR2(1000);
		vSupported  	VARCHAR2(1000);
		vAll			VARCHAR2(500);
		vOracle			VARCHAR2(500);
		vOracleTarget	VARCHAR2(500);
		nRows			INTEGER;
		nCur			INTEGER;
		curDataTypes	INTEGER;
		recOwner		SYS.DBA_TAB_COLS.OWNER%TYPE;
		recTable		SYS.DBA_TAB_COLS.TABLE_NAME%TYPE;
		recColumn		SYS.DBA_TAB_COLS.COLUMN_NAME%TYPE;
		recDataType		SYS.DBA_TAB_COLS.DATA_TYPE%TYPE;
	BEGIN
		/* Checks for unsupported datatypes, based on source version and target */
		vAll          := 'NUMBER,FLOAT,VARCHAR2,VARCHAR,CHAR,NCHAR,NVARCHAR2,DATE,RAW'; 	/* Suported Regardless of Source and Target */
		vOracle       := 'LONG,LONG RAW'; 													/* Supported ONLY if source is > Oracle 9 */
		vOracleTarget := 'BFILE,CLOB,BLOB,NCLOB'; 											/* Supported ONLY if Target is Oracle */
		
		vOwnerString := REPLACE(vOwners,',',chr(39)||','||chr(39));
		vSQL := 'SELECT owner, table_name, column_name, data_type FROM sys.dba_tab_cols WHERE ';
		vSQL := vSQL||'owner IN ('||chr(39)||vOwnerString||chr(39)||') AND table_name NOT LIKE '||chr(39)||'BIN$%'||chr(39)||' ';

		IF vTarget = 'ORACLE' AND nMajor > 9 THEN
		    vSupported := vAll||','||vOracle||','||vOracleTarget;
  	  		vWhere := ' AND data_type NOT LIKE '||chr(39)||'INTERVAL%'||chr(39)||' AND data_type NOT LIKE '||chr(39)||'TIMESTAMP%'||chr(39);
		ELSIF vTarget = 'ORACLE' AND nMajor = 9 THEN
			vSupported := vAll||','||vOracleTarget;
  	  		vWhere := ' AND data_type NOT LIKE '||chr(39)||'INTERVAL%'||chr(39)||' AND data_type NOT LIKE '||chr(39)||'TIMESTAMP%'||chr(39);
		ELSIF vTarget IN ('MYSQL55','MYSQL56','MYSQL57','MSSQL') AND nMajor > 9 THEN
			vSupported := vAll||','||vOracle;
			
			IF vTarget != 'MYSQL55' THEN
				/* TIMESTAMP Only supported with a MySQL Target of 5.6+ */
				vWhere := ' AND data_type NOT LIKE '||chr(39)||'TIMESTAMP%'||chr(39);
			ELSE
				vWhere := '';
			END IF;
		ELSE 
			vSupported := vAll;
			vWhere := '';
		END IF;
		
		vSupported := REPLACE(vSupported,',',chr(39)||','||chr(39));
		
		vSQL := vSQL||'AND data_type NOT IN ('||chr(39)||vSupported||chr(39)||')'||vWhere;
		
		curDataTypes := DBMS_SQL.OPEN_CURSOR;
		DBMS_SQL.PARSE(curDataTypes, vSQL, DBMS_SQL.NATIVE);
		DBMS_SQL.DEFINE_COLUMN(curDataTypes, 1, recOwner, 30);
		DBMS_SQL.DEFINE_COLUMN(curDataTypes, 2, recTable, 30);
		DBMS_SQL.DEFINE_COLUMN(curDataTypes, 3, recColumn, 30);
		DBMS_SQL.DEFINE_COLUMN(curDataTypes, 4, recDataType, 106);
		
		nCur := DBMS_SQL.EXECUTE(curDataTypes);
		nDataTypeOK := 1;
		nRows := 0;
		
		LOOP
			EXIT WHEN DBMS_SQL.FETCH_ROWS(curDataTypes) = 0;
					
				nDataTypeOK := 0;
				nRows := nRows + 1;
				DBMS_SQL.COLUMN_VALUE(curDataTypes, 1, recOwner);
				DBMS_SQL.COLUMN_VALUE(curDataTypes, 2, recTable);
				DBMS_SQL.COLUMN_VALUE(curDataTypes, 3, recColumn);
				DBMS_SQL.COLUMN_VALUE(curDataTypes, 4, recDataType);
					
				logResult(vLogFile, 'ERROR >> Table ['||recOwner||'.'||recTable||'] : Column ['||recColumn||'] is of unsupported datatype ['||recDataType||']');
		END LOOP;

		DBMS_SQL.CLOSE_CURSOR(curDataTypes);

	EXCEPTION
		WHEN OTHERS THEN
			IF DBMS_SQL.IS_OPEN(curDataTypes) THEN
				DBMS_SQL.CLOSE_CURSOR(curDataTypes);
			END IF;
		
			nDataTypeOK := -1;
	END checkDataTypes;
	
	PROCEDURE checkPKExists (
		nPKCheckOK  OUT NUMBER ,
		vOwners     IN  VARCHAR2,
		vLogFile	IN  VARCHAR2 ) IS
		vSQL			VARCHAR2(32767);
		vWhere			VARCHAR2(2000);
		vOwnerString 	VARCHAR2(1000);
		nRows			INTEGER;
		nCur			INTEGER;
		curPK 			INTEGER;
		recOwner		SYS.DBA_TABLES.OWNER%TYPE;
		recTable		SYS.DBA_TABLES.TABLE_NAME%TYPE;
	BEGIN
		/* Checks for tables that don't have Primary Keys */
	
		vOwnerString := REPLACE(vOwners,',',chr(39)||','||chr(39));
		vSQL := 'SELECT owner, table_name FROM sys.dba_tables WHERE owner IN ('||chr(39)||vOwnerString||chr(39)||') ';
		vSQL := vSQL||'AND table_name NOT IN (SELECT DISTINCT table_name FROM dba_constraints ';
		vSQL := vSQL||'WHERE owner IN ('||chr(39)||vOwnerString||chr(39)||') AND constraint_type = '||chr(39)||'P'||chr(39)||' ';
		vSQL := vSQL||'AND table_name NOT LIKE '||chr(39)||'BIN$%'||chr(39)||')';
	
		curPK := DBMS_SQL.OPEN_CURSOR;
		DBMS_SQL.PARSE(curPK, vSQL, DBMS_SQL.NATIVE);
		DBMS_SQL.DEFINE_COLUMN(curPK, 1, recOwner, 30);
		DBMS_SQL.DEFINE_COLUMN(curPK, 2, recTable, 30);
	
		nCur := DBMS_SQL.EXECUTE(curPK);
		nPKCheckOK := 1;
		nRows := 0;
	
		LOOP
			EXIT WHEN DBMS_SQL.FETCH_ROWS(curPK) = 0;
				
				nPKCheckOK := 0;
				nRows := nRows + 1;
				DBMS_SQL.COLUMN_VALUE(curPK, 1, recOwner);
				DBMS_SQL.COLUMN_VALUE(curPK, 2, recTable);
			
				logResult(vLogFile, 'ERROR >> Table ['||recOwner||'.'||recTable||'] does not have a Primary Key');
		END LOOP;

		DBMS_SQL.CLOSE_CURSOR(curPK);

	EXCEPTION
		WHEN OTHERS THEN
			IF DBMS_SQL.IS_OPEN(curPK) THEN
				DBMS_SQL.CLOSE_CURSOR(curPK);
			END IF;
	
			nPKCheckOK := -1;
	END checkPKExists;
	
	PROCEDURE checkFeatures (
		nFeaturesOK		OUT NUMBER,
		vOwners			IN	VARCHAR2,
		vLogFile		IN	VARCHAR2) IS
		nClusterCheck	NUMBER;
		vSQL			VARCHAR2(32767);
		vOwnerString 	VARCHAR2(1000);
		nCur			INTEGER;
		curFeatures		INTEGER;
		recOwner		SYS.DBA_TABLES.OWNER%TYPE;
		recTable		SYS.DBA_TABLES.TABLE_NAME%TYPE;		
	BEGIN
	
		vOwnerString := REPLACE(vOwners,',',chr(39)||','||chr(39));
		
		/* Check for Clusered Tables */
		vSQL := 'SELECT owner, table_name FROM sys.dba_tables WHERE owner IN ('||chr(39)||vOwnerString||chr(39)||') ';
		vSQL := vSQL||'AND cluster_name IS NOT NULL AND table_name NOT LIKE '||chr(39)||'BIN$%'||chr(39)||' ';

		curFeatures := DBMS_SQL.OPEN_CURSOR;
		DBMS_SQL.PARSE(curFeatures, vSQL, DBMS_SQL.NATIVE);
		DBMS_SQL.DEFINE_COLUMN(curFeatures, 1, recOwner, 30);
		DBMS_SQL.DEFINE_COLUMN(curFeatures, 2, recTable, 30);

		nCur := DBMS_SQL.EXECUTE(curFeatures);
		nFeaturesOK := 1;

		LOOP
			EXIT WHEN DBMS_SQL.FETCH_ROWS(curFeatures) = 0;
			
				nFeaturesOK := 0;
				DBMS_SQL.COLUMN_VALUE(curFeatures, 1, recOwner);
				DBMS_SQL.COLUMN_VALUE(curFeatures, 2, recTable);
		
				logResult(vLogFile, 'ERROR >> Table ['||recOwner||'.'||recTable||'] is an unsupported Cluster Table');
		END LOOP;

		DBMS_SQL.CLOSE_CURSOR(curFeatures);
		/* End Cluster table Check */
		
		
	END checkFeatures;
	
	PROCEDURE checkUser (
		nUserOK			OUT NUMBER,
		vReplUser		IN	VARCHAR2,
		vLogFile		IN  VARCHAR2 ) IS
		nCheck			NUMBER;
		nConnectPrivs	NUMBER;
	BEGIN
		nCheck := 0;
		nUserOK := 1;
		
		/* First, check the user exists */
		SELECT COUNT(*) INTO nCheck FROM DBA_USERS WHERE USERNAME = vReplUser;
		
		IF nCheck > 0 THEN
			nCheck := 0;
			logResult(vLogFile, 'INFO  >> User ['||vReplUser||'] exists');
			/* Check is user has DBA Role, in which case no more checks required... */
			SELECT COUNT(*) INTO nCheck FROM DBA_ROLE_PRIVS WHERE GRANTEE = vReplUser AND GRANTED_ROLE = 'DBA';
			
			IF nCheck = 0 THEN
				/* Check the user has either CONNECT role, or the privileges associated with the role */
				SELECT COUNT(*) INTO nConnectPrivs FROM ROLE_SYS_PRIVS WHERE ROLE = 'CONNECT';
			
				SELECT COUNT(*) INTO nCheck FROM DBA_ROLE_PRIVS WHERE GRANTEE = vReplUser AND GRANTED_ROLE = 'CONNECT';
			
				IF nCheck = 0 THEN
					nCheck := 0;
					logResult(vLogFile, 'WARN  >> ['||vReplUser||'] user does not have CONNECT Role');
				
					SELECT COUNT(*) INTO nCheck FROM DBA_SYS_PRIVS WHERE GRANTEE = vReplUser AND PRIVILEGE IN (SELECT PRIVILEGE FROM ROLE_SYS_PRIVS WHERE ROLE = 'CONNECT');
			
					IF nCheck = 0 OR nCheck != nConnectPrivs THEN
						nUserOK := 0;
						logResult(vLogFile,'ERROR >> ['||vReplUser||'] user does not have CONNECT Role, or equivalent CREATE SESSION privileges');
					ELSE
						logResult(vLogFile,'INFO  >> ['||vReplUser||'] user does not have CONNECT Role, but DOES have necessary CREATE SESSION privileges');
					END IF;
				ELSE
					logResult(vLogFile,'INFO  >> ['||vReplUser||'] user has CONNECT privileges');
				END IF;
			
				/* Next, check for CREATE TABLE privilege */
			
				nCheck := 0;	
				SELECT COUNT(*) INTO nCheck FROM DBA_SYS_PRIVS WHERE GRANTEE = vReplUser AND PRIVILEGE = 'CREATE TABLE';
			
				IF nCheck = 0 THEN
					nUserOK := 0;
					logResult(vLogFile,'ERROR >> ['||vReplUser||'] user does not have CREATE TABLE privileges');
				ELSE
					logResult(vLogFile,'INFO  >> ['||vReplUser||'] user has CREATE TABLE privileges');
				END IF;
				
				/* Next, check for CREATE VIEW privilege */
			
				nCheck := 0;	
				SELECT COUNT(*) INTO nCheck FROM DBA_SYS_PRIVS WHERE GRANTEE = vReplUser AND PRIVILEGE = 'CREATE VIEW';
					
				IF nCheck = 0 THEN
					nUserOK := 0;
					logResult(vLogFile,'ERROR >> ['||vReplUser||'] user does not have CREATE VIEW privileges');
				ELSE
					logResult(vLogFile,'INFO  >> ['||vReplUser||'] user has CREATE VIEW privileges');
				END IF;

				/* Finally, check for execute on DBMS_FLASHBACK */
		
				nCheck := 0;	
				SELECT COUNT(*) INTO nCheck FROM DBA_TAB_PRIVS WHERE GRANTEE = vReplUser AND TABLE_NAME = 'DBMS_FLASHBACK' AND PRIVILEGE = 'EXECUTE';

				IF nCheck = 0 THEN
					nUserOK := 0;
					logResult(vLogFile,'ERROR >> ['||vReplUser||'] user does not have GRANT EXECUTE ON DBMS_FLASHBACK privilege');
				ELSE
					logResult(vLogFile,'INFO  >> ['||vReplUser||'] user has GRANT EXECUTE ON DBMS_FLASHBACK privilege');
				END IF;
			ELSE
				nUserOK :=1;
				logResult(vLogFile, 'INFO  >> ['||vReplUser||'] user has DBA role');
			END IF;
		ELSE
			nUserOK := 0;
			logResult(vLogFile, 'ERROR >> User ['||vReplUser||'] does not exist');
		END IF;
	END;
		
    PROCEDURE run (vTarget IN VARCHAR2, vOwners IN VARCHAR2, vReplUser IN VARCHAR2, vLogDir IN VARCHAR2 ) IS
		nVersionOK  NUMBER;
		nDataTypeOK NUMBER;
		nPKCheckOK	NUMBER;
		nFeaturesOK NUMBER;
		nUserOK		NUMBER;
		nMajor      NUMBER;
		nMinor      NUMBER;
		vState		VARCHAR2(9);
		vLogFile	VARCHAR2(25);
	BEGIN
	
		IF UPPER(vTarget) NOT IN ('ORACLE','MYSQL55','MYSQL56','MYSQL57','MSSQL') THEN
			raise_application_error(-20000, 'Invalid <target> supplied, must be one of ORACLE, MYSQL55 or MYSQL56');
		END IF;
		
		configLogDir (vLogDir);
		
		/* Generate file name for output */
		SELECT 'prereq-'||TO_CHAR(SYSDATE,'YYYYMMDDHH24MI')||'.log' INTO vLogFile FROM DUAL;
		
		/* First Get (and Check) the Version of Oracle is 9.2 or greater */
		getVersion(nVersionOK, nMajor, nMinor, vLogFile);
		
		SELECT DECODE(nVersionOK,1,'PASS  >> ','FAIL  >> ') INTO vState FROM DUAL;
		
		logResult(vLogFile, vState||'Source Version Check ['||nVersionOK||']');
		/* End Version Check */
		
		/* Based on Version and Target, check for unsupported Data Types */
		checkDataTypes(nDataTypeOK, UPPER(vTarget), UPPER(vOwners), nMajor, nMinor, vLogFile);
		
		SELECT DECODE(nDataTypeOK,1,'PASS  >> ','FAIL  >> ') INTO vState FROM DUAL;
		
		logResult(vLogFile, vState||'Data Type Check ['||nDataTypeOK||']');
		/* End Data Type Check */
		
		/* Check for any tables that doen't have Primary Keys */
		checkPKExists(nPKCheckOK, UPPER(vOwners), vLogFile);
		
		SELECT DECODE(nPKCheckOK,1,'PASS  >> ','FAIL  >> ') INTO vState FROM DUAL;
		
		logResult(vLogFile, vState||'Primary Key Check ['||nPKCheckOK||']');
		/* End Primary Key Check */

		/* Check for unsupported features */
		checkFeatures(nFeaturesOK, UPPER(vOwners), vLogFile);
		
		SELECT DECODE(nFeaturesOK,1,'PASS  >> ','FAIL  >> ') INTO vState FROM DUAL;
		
		logResult(vLogFile, vState||'Unsupported Features Check ['||nFeaturesOK||']');		
		/* End Features Check */

		/* Check user exists and has correct privs */
		checkUser(nUserOK, UPPER(vReplUser), vLogFile);
		
		SELECT DECODE(nUserOK,1,'PASS  >> ','FAIL  >> ') INTO vState FROM DUAL;
		
		logResult(vLogFile, vState||'User Check ['||nUserOK||']');		
		/* End User Check */
		
		dbms_output.put_line ('>> Checks complete, results in '||vLogDir||'/'||vLogFile);
		
	END run;
	
	PROCEDURE configLogDir (vLogDir IN VARCHAR2) IS
		vSQL 	VARCHAR2(2000);
		curDIR	INTEGER;
		nCount	NUMBER;
		x		INTEGER;
	BEGIN
		
		SELECT COUNT(*) INTO nCount FROM ALL_DIRECTORIES WHERE DIRECTORY_NAME = 'CONTINUENT_DIR';
		
		IF nCount > 0 THEN
			vSQL := 'DROP DIRECTORY CONTINUENT_DIR';
		
			curDIR := DBMS_SQL.OPEN_CURSOR;
			DBMS_SQL.PARSE(curDIR, vSQL, DBMS_SQL.NATIVE);
			x := DBMS_SQL.EXECUTE(curDIR);
			DBMS_SQL.CLOSE_CURSOR(curDIR);
		END IF;
			
		vSQL := 'CREATE DIRECTORY CONTINUENT_DIR AS '||chr(39)||vLogDir||chr(39);
		curDIR := DBMS_SQL.OPEN_CURSOR;
		DBMS_SQL.PARSE(curDIR, vSQL, DBMS_SQL.NATIVE);
		x := DBMS_SQL.EXECUTE(curDIR);
		DBMS_SQL.CLOSE_CURSOR(curDIR);
	END;
		
	PROCEDURE logResult (vLogFile IN VARCHAR2, vLogString IN VARCHAR2) IS
		fileHandler	UTL_FILE.FILE_TYPE;
	BEGIN
		fileHandler := UTL_FILE.FOPEN('CONTINUENT_DIR', vLogFile, 'a');
		
		UTL_FILE.PUT_LINE(fileHandler, vLogString, TRUE);
		
		UTL_FILE.FCLOSE(fileHandler);
	END logResult;
		
END ContinuentPreReqCheck;
/


/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.mysql;

import com.google.common.collect.Maps;
import io.dingodb.DingoClient;
import io.dingodb.client.Record;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MysqlInit {

    static DingoClient mysqlClient = null;

    static DingoClient informationClient = null;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -cp mysql-init.jar io.dingodb.mysql.MysqlInit <coordinatorSvr>");
            return;
        }
        String coordinatorSvr = args[0];
        System.out.println("coordinator:" + coordinatorSvr);
        mysqlClient = new DingoClient(coordinatorSvr, "mysql", 10);
        mysqlClient.open();
        informationClient = new DingoClient(coordinatorSvr, "information_schema", 10);
        informationClient.open();

        initUser("USER");
        initDb("DB");
        initTablesPriv("TABLES_PRIV");
        initGlobalVariables("GLOBAL_VARIABLES");
        mysqlClient.close();
        informationClient.close();
    }

    public static void initUser(String tableName) {
        ColumnDefinition c1 = new ColumnDefinition("Host", "varchar", "", 0, 60, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("User", "varchar", "", 0, 32, false, 1, "");
        ColumnDefinition c3 = new ColumnDefinition("Select_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c4 = new ColumnDefinition("Insert_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c5 = new ColumnDefinition("Update_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c6 = new ColumnDefinition("Delete_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c7 = new ColumnDefinition("Create_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c8 = new ColumnDefinition("Drop_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c9 = new ColumnDefinition("Reload_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c10 = new ColumnDefinition("Shutdown_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c11 = new ColumnDefinition("Process_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c12 = new ColumnDefinition("File_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c13 = new ColumnDefinition("Grant_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c14 = new ColumnDefinition("References_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c15 = new ColumnDefinition("Index_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c16 = new ColumnDefinition("Alter_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c17 = new ColumnDefinition("Show_db_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c18 = new ColumnDefinition("Super_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c19 = new ColumnDefinition("Create_tmp_table_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c20 = new ColumnDefinition("Lock_tables_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c21 = new ColumnDefinition("Execute_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c22 = new ColumnDefinition("Repl_slave_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c23 = new ColumnDefinition("Repl_client_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c24 = new ColumnDefinition("Create_view_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c25 = new ColumnDefinition("Show_view_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c26 = new ColumnDefinition("Create_routine_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c27 = new ColumnDefinition("Alter_routine_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c28 = new ColumnDefinition("Create_user_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c29 = new ColumnDefinition("Event_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c30 = new ColumnDefinition("Trigger_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c31 = new ColumnDefinition("Create_tablespace_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c32 = new ColumnDefinition("ssl_type", "varchar", "", 0, 32, false, -1, "");
        ColumnDefinition c33 = new ColumnDefinition("ssl_cipher", "varchar", "", 0, 65535, false, -1, "");
        ColumnDefinition c34 = new ColumnDefinition("x509_issuer", "varchar", "", 0, 65535, false, -1, "");
        ColumnDefinition c35 = new ColumnDefinition("x509_subject", "varchar", "", 0, 65535, false, -1, "");
        ColumnDefinition c36 = new ColumnDefinition("max_questions", "integer", "", 0, 11, false, -1, "0");
        ColumnDefinition c37 = new ColumnDefinition("max_updates", "integer", "", 0, 11, false, -1, "0");
        ColumnDefinition c38 = new ColumnDefinition("max_connections", "integer", "", 0, 11, false, -1, "0");
        ColumnDefinition c39 = new ColumnDefinition("max_user_connections", "integer", "", 0, 11, false, -1, "0");
        ColumnDefinition c40 = new ColumnDefinition("plugin", "varchar", "", 0, 64, false, -1, "mysql_native_password");
        ColumnDefinition c41 = new ColumnDefinition("authentication_string", "varchar", "", 0, 65535, true, -1, "");
        ColumnDefinition c42 = new ColumnDefinition("password_expired", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c43 = new ColumnDefinition("password_last_changed", "timestamp", "", 0, 0, true, -1, "");
        ColumnDefinition c44 = new ColumnDefinition("password_lifetime", "integer", "", 0, 5, true, -1, "");
        ColumnDefinition c45 = new ColumnDefinition("account_locked", "varchar", "", 0, 2, false, -1, "N");

        TableDefinition tableDefinition = new TableDefinition(tableName,
                Arrays.asList(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17,
                        c18, c19, c20, c21, c22, c23,
                        c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38, c39, c40, c41, c42,
                        c43, c44, c45),
                1,
                0,
                null,
                Common.Engine.ENG_ROCKSDB.name(),
                null);
        boolean isSuccess = mysqlClient.createTable(tableDefinition);
        System.out.println("create user table is success:" + isSuccess);
        sleep();

        LinkedHashMap<String, Object> map = Maps.newLinkedHashMap();
        map.put(c1.getName(), "%");
        map.put(c2.getName(), "root");
        List<ColumnDefinition> privilegeColumns = Arrays.asList(c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14,
                c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31);
        for (ColumnDefinition columnDefinition : privilegeColumns) {
            map.put(columnDefinition.getName(), "Y");
        }
        List<ColumnDefinition> nullColumns = Arrays.asList(c32, c33, c34, c35);
        for (ColumnDefinition columnDefinition : nullColumns) {
            map.put(columnDefinition.getName(), "");
        }
        map.put(c36.getName(), 0);
        map.put(c37.getName(), 0);
        map.put(c38.getName(), 0);
        map.put(c39.getName(), 0);
        map.put(c40.getName(), "mysql_native_password");
        map.put(c41.getName(), "");
        map.put(c42.getName(), "N");
        map.put(c43.getName(), new Timestamp(System.currentTimeMillis()));
        map.put(c44.getName(), 0);
        map.put(c45.getName(), "N");

        List<String> keyNames = Arrays.asList(c1.getName(), c2.getName());

        boolean result = mysqlClient.upsert(tableName, new Record(keyNames, map));
        if (result) {
            System.out.println("init root user success");
        } else {
            System.out.println("init root user fail");
        }
    }

    public static void initDb(String tableName) {
        ColumnDefinition c1 = new ColumnDefinition("Host", "varchar", "", 0, 60, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("User", "varchar", "", 0, 64, false, 1, "");
        ColumnDefinition c3 = new ColumnDefinition("Db", "varchar", "", 0, 32, false, 2, "");
        ColumnDefinition c4 = new ColumnDefinition("Select_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c5 = new ColumnDefinition("Insert_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c6 = new ColumnDefinition("Update_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c7 = new ColumnDefinition("Delete_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c8 = new ColumnDefinition("Create_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c9 = new ColumnDefinition("Drop_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c10 = new ColumnDefinition("Grant_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c11 = new ColumnDefinition("References_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c12 = new ColumnDefinition("Index_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c13 = new ColumnDefinition("Alter_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c14 = new ColumnDefinition("Create_tmp_table_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c15 = new ColumnDefinition("Lock_tables_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c16 = new ColumnDefinition("Create_view_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c17 = new ColumnDefinition("Show_view_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c18 = new ColumnDefinition("Create_routine_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c19 = new ColumnDefinition("Alter_routine_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c20 = new ColumnDefinition("Execute_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c21 = new ColumnDefinition("Event_priv", "varchar", "", 0, 2, false, -1, "N");
        ColumnDefinition c22 = new ColumnDefinition("Trigger_priv", "varchar", "", 0, 2, false, -1, "N");

        TableDefinition tableDefinition = new TableDefinition(tableName,
                Arrays.asList(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17,
                        c18, c19, c20, c21, c22),
                1,
                0,
                null,
                Common.Engine.ENG_ROCKSDB.name(),
                null);
        boolean isSuccess = mysqlClient.createTable(tableDefinition);
        System.out.println("create db privilege result:" + isSuccess);
    }

    public static void initTablesPriv(String tableName) {
        ColumnDefinition c1 = new ColumnDefinition("Host", "varchar", "", 0, 60, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("User", "varchar", "", 0, 32, false, 1, "");
        ColumnDefinition c3 = new ColumnDefinition("Db", "varchar", "", 0, 64, false, 2, "");
        ColumnDefinition c4 = new ColumnDefinition("Table_name", "varchar", "", 0, 64, false, 3, "");
        ColumnDefinition c5 = new ColumnDefinition("Grantor", "varchar", "", 0, 93, true, -1, "N");
        ColumnDefinition c6 = new ColumnDefinition("Timestamp", "timestamp", "", 0, 0, true, -1, "N");
        ColumnDefinition c7 = new ColumnDefinition("Table_priv", "varchar", "", 0, 1024, true, -1, "N");
        ColumnDefinition c8 = new ColumnDefinition("Column_priv", "varchar", "", 0, 1024, true, -1, "N");

        TableDefinition tableDefinition = new TableDefinition(tableName,
                Arrays.asList(c1, c2, c3, c4, c5, c6, c7, c8),
                1,
                0,
                null,
                Common.Engine.ENG_ROCKSDB.name(),
                null);
        boolean isSuccess = mysqlClient.createTable(tableDefinition);
        System.out.println("create table privilege result:" + isSuccess);
    }

    public static void initGlobalVariables(String tableName) {
        ColumnDefinition c1 = new ColumnDefinition("VARIABLE_NAME", "varchar", "", 0, 64, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("VARIABLE_VALUE", "varchar", "", 0, 1024, true, -1, "");
        TableDefinition tableDefinition = new TableDefinition(tableName,
                Arrays.asList(c1, c2),
                1,
                0,
                null,
                Common.Engine.ENG_ROCKSDB.name(),
                null);
        boolean isSuccess = informationClient.createTable(tableDefinition);
        System.out.println("create global variables result:" + isSuccess);
    }

    private static void sleep() {
        try {
            Thread.sleep(25 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
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

package com.amazon.carbonado.repo.sleepycat;

import java.io.FileNotFoundException;

import com.sleepycat.db.CompactStats;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.raw.StorableCodecFactory;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * Storage implementation for DBRepository.
 *
 * @author Brian S O'Neill
 * @author Vidya Iyer
 * @author Nicole Deflaux
 */
class DB_Storage<S extends Storable> extends BDBStorage<Transaction, S> {
    // Primary database of Storable instances
    private Database mDatabase;

    /**
     *
     * @param repository repository reference
     * @param storableFactory factory for emitting storables
     * @param db database for Storables
     * @throws DatabaseException
     * @throws SupportException
     */
    DB_Storage(DB_Repository repository, Class<S> type)
        throws DatabaseException, RepositoryException
    {
        super(repository, type);
        open(repository.mReadOnly);
    }

    @Override
    protected boolean db_exists(Transaction txn, byte[] key, boolean rmw) throws Exception {
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        DatabaseEntry dataEntry = new DatabaseEntry();
        dataEntry.setPartial(0, 0, true);
        OperationStatus status = mDatabase.get
            (txn, keyEntry, dataEntry, rmw ? LockMode.RMW : null);
        return status != OperationStatus.NOTFOUND;
    }

    @Override
    protected byte[] db_get(Transaction txn, byte[] key, boolean rmw) throws Exception {
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        DatabaseEntry dataEntry = new DatabaseEntry();
        OperationStatus status = mDatabase.get
            (txn, keyEntry, dataEntry, rmw ? LockMode.RMW : null);
        if (status == OperationStatus.NOTFOUND) {
            return NOT_FOUND;
        }
        return dataEntry.getData();
    }

    @Override
    protected Object db_putNoOverwrite(Transaction txn, byte[] key, byte[] value)
        throws Exception
    {
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        DatabaseEntry dataEntry = new DatabaseEntry(value);
        OperationStatus status = mDatabase.putNoOverwrite(txn, keyEntry, dataEntry);
        if (status == OperationStatus.SUCCESS) {
            return SUCCESS;
        } else if (status == OperationStatus.KEYEXIST) {
            return KEY_EXIST;
        } else {
            return NOT_FOUND;
        }
    }

    @Override
    protected boolean db_put(Transaction txn, byte[] key, byte[] value)
        throws Exception
    {
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        DatabaseEntry dataEntry = new DatabaseEntry(value);
        return mDatabase.put(txn, keyEntry, dataEntry) == OperationStatus.SUCCESS;
    }

    @Override
    protected boolean db_delete(Transaction txn, byte[] key) throws Exception {
        DatabaseEntry keyEntry = new DatabaseEntry(key);
        return mDatabase.delete(txn, keyEntry) == OperationStatus.SUCCESS;
    }

    @Override
    protected void db_truncate(Transaction txn) throws Exception {
        if (txn != null) {
            mDatabase.truncate(txn, false);
            return;
        }


        DB_Repository dbRepository = (DB_Repository) getRepository();

        String fileName, dbName;
        {
            StorableCodecFactory codecFactory = dbRepository.getStorableCodecFactory();
            String name = codecFactory.getStorageName(getStorableType());
            if (name == null) {
                name = getStorableType().getName();
            }
            fileName = dbRepository.getDatabaseFileName(name);
            dbName = dbRepository.getDatabaseName(name);
        }

        close();
        dbRepository.mEnv.removeDatabase(null, fileName, dbName);
        open(false, null, false);
    }

    @Override
    protected boolean db_isEmpty(Transaction txn, Object database, boolean rmw) throws Exception {
        Cursor cursor = ((Database) database).openCursor(txn, null);
        OperationStatus status = cursor.getFirst
            (new DatabaseEntry(), new DatabaseEntry(), rmw ? LockMode.RMW : null);
        cursor.close();
        return status == OperationStatus.NOTFOUND;
    }

    @Override
    protected CompactionCapability.Result<S> db_compact
        (Transaction txn, Object database, byte[] start, byte[] end)
        throws Exception
    {
        DatabaseEntry dstart = start == null ? null : new DatabaseEntry(start);
        DatabaseEntry dstop = end == null ? null : new DatabaseEntry(end);

        final CompactStats stats = ((Database) database).compact(txn, dstart, dstop, null, null);

        return new CompactionCapability.Result<S>() {
            public int getPagesExamine() {
                return stats.getPagesExamine();
            }

            public int getPagesFree() {
                return stats.getPagesFree();
            }

            public int getPagesTruncated() {
                return stats.getPagesTruncated();
            }

            public int getLevels() {
                return stats.getLevels();
            }

            public int getDeadlockCount() {
                return stats.getDeadlock();
            }

            @Override
            public String toString() {
                return stats.toString();
            }
        };
    }

    @Override
    protected void db_close(Object database) throws Exception {
        ((Database) database).close();
    }

    @Override
    protected Object env_openPrimaryDatabase(Transaction txn, String name)
        throws Exception
    {
        DB_Repository dbRepository = (DB_Repository) getRepository();

        Environment env = dbRepository.mEnv;
        boolean readOnly = dbRepository.mReadOnly;

        DatabaseConfig config;
        try {
            config = (DatabaseConfig) dbRepository.getInitialDatabaseConfig();
        } catch (ClassCastException e) {
            throw new ConfigurationException
                ("Unsupported initial environment config. Must be instance of " +
                 DatabaseConfig.class.getName(), e);
        }

        if (config == null) {
            config = new DatabaseConfig();
            config.setType(DatabaseType.BTREE);
            config.setSortedDuplicates(false);
            Integer pageSize = dbRepository.getDatabasePageSize(getStorableType());
            if (pageSize != null) {
                config.setPageSize(pageSize);
            }
            if (dbRepository.mChecksum != null) {
                config.setChecksum(dbRepository.mChecksum);
            }
        } else {
            if (DatabaseType.BTREE != config.getType()) {
                throw new IllegalArgumentException("DatabaseConfig: database type is not BTREE");
            }
            if (config.getSortedDuplicates()) {
                throw new IllegalArgumentException("DatabaseConfig: getSortedDuplicates is true");
            }
        }

        // Overwrite these settings as they depend upon the
        // configuration of the repository
        config.setTransactional(dbRepository.mDatabasesTransactional);
        config.setReadOnly(readOnly);
        config.setAllowCreate(!readOnly);

        runDatabasePrepareForOpeningHook(config);

        String fileName = dbRepository.getDatabaseFileName(name);
        String dbName = dbRepository.getDatabaseName(name);
        try {
            return mDatabase = env.openDatabase(txn, fileName, dbName, config);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(e.getMessage() + ": " + fileName);
        }
    }

    @Override
    protected void env_removeDatabase(Transaction txn, String databaseName) throws Exception {
        DB_Repository dbRepository = (DB_Repository) getRepository();
        String fileName = dbRepository.getDatabaseFileName(databaseName);
        String dbName = dbRepository.getDatabaseName(databaseName);
        mDatabase.getEnvironment().removeDatabase(txn, fileName, dbName);
    }

    @Override
    protected BDBCursor<Transaction, S> openCursor
        (TransactionScope<Transaction> scope,
         byte[] startBound, boolean inclusiveStart,
         byte[] endBound, boolean inclusiveEnd,
         int maxPrefix,
         boolean reverse,
         Object database)
        throws Exception
    {
        return new DB_Cursor<S>
            (scope,
             startBound, inclusiveStart,
             endBound, inclusiveEnd,
             maxPrefix,
             reverse,
             this,
             (Database) database);
    }
}

/*
 * Copyright 2012 Amazon Technologies, Inc. or its affiliates.
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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Transaction;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * Storage implementation for DBX_Repository.
 * 
 *
 * @author Brian S O'Neill
 */
class DBX_Storage<S extends Storable> extends DB_Storage<S> {
    final ReadWriteLock mRWLock;

    DBX_Storage(DBX_Repository repository, Class<S> type)
        throws DatabaseException, RepositoryException
    {
        super(repository, type);
        mRWLock = repository.mRWLock;
    }

    @Override
    protected boolean db_exists(Transaction txn, byte[] key, boolean rmw) throws Exception {
        Lock lock = mRWLock.readLock();
        lock.lock();
        try {
            return super.db_exists(txn, key, rmw);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected byte[] db_get(Transaction txn, byte[] key, boolean rmw) throws Exception {
        Lock lock = mRWLock.readLock();
        lock.lock();
        try {
            return super.db_get(txn, key, rmw);
        } finally {
            lock.unlock();
        }
    }

    /*
      Note: The exclusive lock is not acquired for operations in explicit
      transactions. Only one transaction is allowed to run at a time anyhow.
      If exclusive lock acquisition was required, a new type of deadlock is
      introduced. The transaction might have a record locked already, and then
      it needs to modify it. If modification required the exclusive lock, it
      might deadlock on a thread which is stuck trying to read the locked
      record. Either thread will get a timeout exception.
    */

    @Override
    protected Object db_putNoOverwrite(Transaction txn, byte[] key, byte[] value)
        throws Exception
    {
        if (txn != null) {
            return super.db_putNoOverwrite(txn, key, value);
        }
        Lock lock = mRWLock.writeLock();
        lock.lock();
        try {
            return super.db_putNoOverwrite(txn, key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected boolean db_put(Transaction txn, byte[] key, byte[] value)
        throws Exception
    {
        if (txn != null) {
            return super.db_put(txn, key, value);
        }
        Lock lock = mRWLock.writeLock();
        lock.lock();
        try {
            return super.db_put(txn, key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected boolean db_delete(Transaction txn, byte[] key) throws Exception {
        if (txn != null) {
            return super.db_delete(txn, key);
        }
        Lock lock = mRWLock.writeLock();
        lock.lock();
        try {
            return super.db_delete(txn, key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected boolean db_isEmpty(Transaction txn, Object database, boolean rmw) throws Exception {
        ReadWriteLock rwLock = mRWLock;
        if (rwLock == null) {
            // Might be called during open sequence.
            return super.db_isEmpty(txn, database, rmw);
        }

        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return super.db_isEmpty(txn, database, rmw);
        } finally {
            lock.unlock();
        }
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
        return new DBX_Cursor<S>
            (scope,
             startBound, inclusiveStart,
             endBound, inclusiveEnd,
             maxPrefix,
             reverse,
             this,
             (Database) database);
    }
}

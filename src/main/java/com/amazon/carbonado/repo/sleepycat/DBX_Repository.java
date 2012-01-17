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

import java.util.ArrayList;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sleepycat.db.Transaction;

import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.spi.ExceptionTransformer;

/**
 * Repository implementation backed by a Berkeley DB, with an exclusive lock
 * held when modifications are made.
 *
 * @author Brian S O'Neill
 */
class DBX_Repository extends DB_Repository {
    // Read lock acquired by get and cursor operations. Write lock acquired
    // when making changes.
    final ReadWriteLock mRWLock = new ReentrantReadWriteLock(false); // unfair

    // Only one transaction allowed at a time. (a simple semaphore)
    private final Lock mTxnLock = new ReentrantLock(true); // fair
    private final Condition mTxnCondition = mTxnLock.newCondition();

    // Top-level transaction currently in progress. Usually a single Transaction instance.
    private Object mActiveTxn;
    private Thread mActiveTxnThread;

    /**
     * Open the repository using the given BDB repository configuration.
     *
     * @throws IllegalArgumentException if name or environment home is null
     * @throws RepositoryException if there is a problem opening the environment
     */
    DBX_Repository(AtomicReference<Repository> rootRef, BDBRepositoryBuilder builder)
        throws RepositoryException
    {
        super(rootRef, builder);
    }

    /**
     * Open the repository using the given BDB repository configuration.
     *
     * @throws IllegalArgumentException if name or environment home is null
     * @throws RepositoryException if there is a problem opening the environment
     */
    DBX_Repository(AtomicReference<Repository> rootRef, BDBRepositoryBuilder builder,
                   ExceptionTransformer exTransformer)
        throws RepositoryException
    {
        super(rootRef, builder, exTransformer);
    }

    @Override
    public BDBProduct getBDBProduct() {
        return BDBProduct.DB;
    }

    @Override
    protected <S extends Storable> BDBStorage<Transaction, S> createBDBStorage(Class<S> type)
        throws Exception
    {
        return new DBX_Storage<S>(this, type);
    }

    @Override
    protected Transaction txn_begin(Transaction parent, IsolationLevel level) throws Exception {
        Lock lock = acquire(parent);
        try {
            Transaction txn = super.txn_begin(parent, level);
            if (lock != null) {
                addActiveTxn(txn);
            }
            return txn;
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    @Override
    protected Transaction txn_begin_nowait(Transaction parent, IsolationLevel level)
        throws Exception
    {
        Lock lock = acquire(parent);
        try {
            Transaction txn = super.txn_begin_nowait(parent, level);
            if (lock != null) {
                addActiveTxn(txn);
            }
            return txn;
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    @Override
    protected void txn_commit(Transaction txn) throws Exception {
        super.txn_commit(txn);
        release(txn);
    }

    @Override
    protected void txn_abort(Transaction txn) throws Exception {
        super.txn_abort(txn);
        release(txn);
    }

    // Returns Lock instance if acquired. Caller must unlock it.
    private Lock acquire(Transaction parent) throws InterruptedException {
        if (parent != null) {
            // If nested, then assume parent (or ancestor) is already the active one.
            return null;
        }
        Thread thread = Thread.currentThread();
        Lock lock = mTxnLock;
        lock.lock();
        try {
            if (mActiveTxnThread == thread) {
                // Multiple top-level transactions in the same thread is okay.
                return lock;
            }
            while (mActiveTxn != null) {
                mTxnCondition.await();
            }
            mActiveTxnThread = thread;
            // Caller calls addActiveTxn.
            return lock;
        } catch (InterruptedException e) {
            lock.unlock();
            throw e;
        }
    }

    // Caller must hold Lock as returned by acquire.
    private void addActiveTxn(Transaction txn) {
        Object active = mActiveTxn;
        if (active == null) {
            mActiveTxn = txn;
        } else if (active instanceof Transaction) {
            ArrayList list = new ArrayList(4);
            list.add(active);
            list.add(txn);
            mActiveTxn = list;
        } else {
            ((ArrayList) active).add(txn);
        }
    }

    private void release(Transaction txn) {
        Lock lock = mTxnLock;
        lock.lock();
        try {
            Object active = mActiveTxn;
            if (txn == active) {
                mActiveTxn = null;
                mActiveTxnThread = null;
                mTxnCondition.signal();
            } else if (active instanceof ArrayList) {
                ArrayList list = (ArrayList) active;
                if (list.remove(txn)) {
                    if (list.isEmpty()) {
                        mActiveTxn = null;
                        mActiveTxnThread = null;
                    }
                    mTxnCondition.signal();
                }
            }
        } finally {
            lock.unlock();
        }
    }
}

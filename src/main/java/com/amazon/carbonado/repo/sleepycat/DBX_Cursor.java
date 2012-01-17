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

import java.util.concurrent.locks.Lock;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Transaction;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.txn.TransactionScope;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see DBX_Storage
 */
class DBX_Cursor<S extends Storable> extends DB_Cursor<S> {
    private final Lock mLock;

    DBX_Cursor(TransactionScope<Transaction> scope,
               byte[] startBound, boolean inclusiveStart,
               byte[] endBound, boolean inclusiveEnd,
               int maxPrefix,
               boolean reverse,
               DBX_Storage<S> storage,
               Database database)
        throws DatabaseException, FetchException
    {
        super(scope, startBound, inclusiveStart, endBound, inclusiveEnd,
              maxPrefix, reverse, storage, database);
        mLock = storage.mRWLock.readLock();
    }

    @Override
    protected boolean cursor_getCurrent() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getCurrent();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getFirst() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getFirst();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getLast() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getLast();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getSearchKeyRange() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getSearchKeyRange();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getNext() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getNext();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getNextDup() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getNextDup();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getPrev() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getPrev();
        } finally {
            mLock.unlock();
        }
    }

    @Override
    protected boolean cursor_getPrevNoDup() throws Exception {
        mLock.lock();
        try {
            return super.cursor_getPrevNoDup();
        } finally {
            mLock.unlock();
        }
    }
}

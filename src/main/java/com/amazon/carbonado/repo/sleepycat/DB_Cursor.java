/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
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

import com.sleepycat.db.Cursor;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.Transaction;
import static com.sleepycat.db.OperationStatus.*;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.IsolationLevel;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.spi.TransactionScope;

/**
 * Cursor for a primary database.
 *
 * @author Brian S O'Neill
 */
class DB_Cursor<S extends Storable> extends BDBCursor<Transaction, S> {
    protected final Database mDatabase;
    private final LockMode mLockMode;
    private final DatabaseEntry mSearchKey;
    private final DatabaseEntry mData;

    protected Cursor mCursor;

    /**
     * @param scope
     * @param startBound specify the starting key for the cursor, or null if first
     * @param inclusiveStart true if start bound is inclusive
     * @param endBound specify the ending key for the cursor, or null if last
     * @param inclusiveEnd true if end bound is inclusive
     * @param maxPrefix maximum expected common initial bytes in start and end bound
     * @param reverse when true, iteration is reversed
     * @param storage
     * @param database primary database to use
     * @throws IllegalArgumentException if any bound is null but is not inclusive
     */
    DB_Cursor(TransactionScope<Transaction> scope,
              byte[] startBound, boolean inclusiveStart,
              byte[] endBound, boolean inclusiveEnd,
              int maxPrefix,
              boolean reverse,
              DB_Storage<S> storage,
              Database database)
        throws DatabaseException, FetchException
    {
        super(scope, startBound, inclusiveStart, endBound, inclusiveEnd,
              maxPrefix, reverse, storage);

        mDatabase = database;
        mLockMode = scope.isForUpdate() ? LockMode.RMW : LockMode.DEFAULT;

        mSearchKey = new DatabaseEntry();
        mData = new DatabaseEntry();
    }

    @Override
    protected byte[] searchKey_getData() {
        return getData(mSearchKey.getData(), mSearchKey.getSize());
    }

    @Override
    protected byte[] searchKey_getDataCopy() {
        return getDataCopy(mSearchKey.getData(), mSearchKey.getSize());
    }

    @Override
    protected void searchKey_setData(byte[] data) {
        mSearchKey.setData(data);
    }

    @Override
    protected void searchKey_setPartial(boolean partial) {
        mSearchKey.setPartial(0, 0, partial);
    }

    @Override
    protected boolean searchKey_getPartial() {
        return mSearchKey.getPartial();
    }

    @Override
    protected byte[] data_getData() {
        return getData(mData.getData(), mData.getSize());
    }

    @Override
    protected byte[] data_getDataCopy() {
        return getDataCopy(mData.getData(), mData.getSize());
    }

    @Override
    protected void data_setPartial(boolean partial) {
        mData.setPartial(0, 0, partial);
    }

    @Override
    protected boolean data_getPartial() {
        return mData.getPartial();
    }

    @Override
    protected byte[] primaryKey_getData() {
        // Search key is primary key.
        return getData(mSearchKey.getData(), mSearchKey.getSize());
    }

    @Override
    protected void cursor_open(Transaction txn, IsolationLevel level) throws Exception {
        CursorConfig config;
        if (level == IsolationLevel.READ_COMMITTED) {
            config = CursorConfig.READ_COMMITTED;
        } else if (level == IsolationLevel.READ_UNCOMMITTED) {
            config = CursorConfig.READ_UNCOMMITTED;
        } else {
            config = CursorConfig.DEFAULT;
        }
        mCursor = mDatabase.openCursor(txn, config);
    }

    @Override
    protected void cursor_close() throws Exception {
        mCursor.close();
    }

    @Override
    protected boolean cursor_getCurrent() throws Exception {
        return mCursor.getCurrent(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getFirst() throws Exception {
        return mCursor.getFirst(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getLast() throws Exception {
        return mCursor.getLast(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getSearchKeyRange() throws Exception {
        return mCursor.getSearchKeyRange(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getNext() throws Exception {
        return mCursor.getNext(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getNextDup() throws Exception {
        return mCursor.getNextDup(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getPrev() throws Exception {
        return mCursor.getPrev(mSearchKey, mData, mLockMode) == SUCCESS;
    }

    @Override
    protected boolean cursor_getPrevNoDup() throws Exception {
        return mCursor.getPrevNoDup(mSearchKey, mData, mLockMode) == SUCCESS;
    }
}

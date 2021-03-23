{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import Data.String.Conversions (cs)
import Database.RocksDB (DB, get, put, delete, withDB, getCF, putCF, txnGetForUpdate,
           createIfMissing, errorIfExists, bloomFilter, maxFiles,
           prefixLength, paranoidChecks, Config(..), columnFamilies, withTxn,
           withDBCF, withTxnDB, txnBegin, txnCommit, txnPut, txnGet, txnDelete)
import Test.Hspec (describe, hspec, it, shouldReturn, shouldBe, shouldThrow)
import UnliftIO (MonadUnliftIO, wait, async, withSystemTempDirectory)
import System.IO.Error (isUserError)

conf :: Config
conf = Config {
             createIfMissing = True
           , errorIfExists   = True
           , bloomFilter     = True
           , maxFiles        = Nothing
           , prefixLength    = Just 3
           , paranoidChecks  = False
           }

withTestDB :: MonadUnliftIO m => FilePath -> (DB -> m a) -> m a
withTestDB path =
    withDB path conf

withTestDBCF :: MonadUnliftIO m => FilePath -> [String] -> (DB -> m a) -> m a
withTestDBCF path cfs =
    withDBCF path conf (map (,conf) cfs)

main :: IO ()
main =  do
    hspec $ do
        describe "Database engine with transactions" $ do
            it "can run operations in a transaction using withTxn" $
                withSystemTempDirectory "rocksdb-txn1" $ \path -> do
                    withTxnDB path conf $ \txnDB -> do
                        withTxn txnDB $ \txn -> txnGet txn txnDB "k1" `shouldReturn` Nothing
                        withTxn txnDB $ \txn -> txnPut txn "k1" "v1"
                        withTxn txnDB $ \txn -> txnGet txn txnDB "k1" `shouldReturn` Just "v1"

            it "does not return consistent reads across multiple keys using txnGet alone" $
                withSystemTempDirectory "rocksdb-txn2" $ \path -> do
                    withTxnDB path conf $ \txnDB -> do
                        -- lets suppose we have two keys, k1:v1 and k2:v2.
                        -- they should be written and read atomically as a pair
                        txn <- txnBegin txnDB
                        txnPut txn "k1" "v1"
                        txnPut txn "k2" "v2"
                        txnCommit txn

                        -- now in one transaction we try to read them,
                        -- expecting them to still be k1:v1 and k2:v2
                        txn2 <- txnBegin txnDB
                        txnGet txn2 txnDB "k1" `shouldReturn` Just "v1"
                        -- now there is a pause in txn2 for whatever reason, maybe we do
                        -- a bit of work processing the result before getting k2.
                        -- and unfortunately exactly in the middle of this pause someone
                        -- else (txn3) comes along and changes k1 and k2

                        -- but if we only used txnGet and not txnGetForUpdate, they were
                        -- not locked and can be changed (including k2 which we have not
                        -- read yet
                        txn3 <- txnBegin txnDB
                        txnPut txn3 "k1" "v1.2"
                        txnPut txn3 "k2" "v2.2"
                        txnCommit txn3

                        -- now txn2 reads k2 which now is v2.2 even though we wanted it to
                        -- still read it as v2
                        txnGet txn2 txnDB "k2" `shouldReturn` Just "v2.2"
                        txnCommit txn2
                    

            it "should lock keys whith txnGetForUpdate" $
                withSystemTempDirectory "rocksdb-txn3" $ \path -> do
                    let k = "k"
                    let v = "v"
                    let v2 = "v2"
                    let v3 = "v3"
                    let v4 = "v4"

                    withTxnDB path conf $ \txnDB -> do
                        txn <- txnBegin txnDB
                        txnPut txn k v
                        txnCommit txn

                        -- lets try to see what happens if we try to write to a locked
                        -- key from outside the transaction that locked it
                        txn2 <- txnBegin txnDB
                        txnGetForUpdate txn2 txnDB k `shouldReturn` (Just v)

                        -- this is a different transaction now, started while txn3 is
                        -- still locked
                        txn3 <- txnBegin txnDB
                        txnGet txn3 txnDB k `shouldReturn` (Just v)
                        txnGetForUpdate txn3 txnDB k `shouldThrow` isUserError

                        -- if we change something in txn2 (not committed yet) we see the
                        -- change in txn2, but txn3 still sees the old version
                        txnPut txn2 k v2
                        txnGetForUpdate txn2 txnDB k `shouldReturn` (Just v2)
                        txnGet txn3 txnDB k `shouldReturn` (Just v)

                        -- as long as txn2 has the lock on the key, nobody else can lock
                        -- it, no matter how hard they try, they will only get exceptions
                        txnGetForUpdate txn3 txnDB k `shouldThrow` isUserError
                        txn4 <- txnBegin txnDB
                        txnGetForUpdate txn4 txnDB k `shouldThrow` isUserError

                        -- if they try to change the locked key as well
                        txnPut txn4 k "x" `shouldThrow` isUserError

                        -- after txn2 commits, we can see the change from outside it
                        txnCommit txn2
                        txnGet txn3 txnDB k `shouldReturn` (Just v2)

                        txnCommit txn3

                        -- now that txn2 has committed, it is again possible to lock the
                        -- key
                        txnGetForUpdate txn4 txnDB k `shouldReturn` (Just v2)
                        txnCommit txn4

                        -- transactions can seemingly be reused but will throw an error
                        -- when trying to commit them a second time
                        txnGetForUpdate txn4 txnDB k `shouldReturn` (Just v2)
                        txnPut txn4 k v3
                        -- after locking the key once with getForUpdate it does not matter
                        -- if we use it or regular get, we can see our change
                        txnGetForUpdate txn4 txnDB k `shouldReturn` (Just v3)
                        txnGet txn4 txnDB k `shouldReturn` (Just v3)

                        -- outside of txn4 the change is not visible yet, of course.
                        txnGet txn3 txnDB k `shouldReturn` (Just v2)

                        -- committing an already committed transaction is illegal
                        txnCommit txn4 `shouldThrow` isUserError

                        -- but the key is still locked
                        txn5 <- txnBegin txnDB
                        txnPut txn5 k "x" `shouldThrow` isUserError
                        -- the lesson learned is that we need to make sure that
                        -- transactions cannot be reused

                    -- if the db is closed and reopened in another handle, uncommitted
                    -- transactions are aborted, letting us access the key again.
                    withTxnDB path (conf{ errorIfExists = False }) $ \txnDB2 -> do
                        txn6 <- txnBegin txnDB2
                        txn7 <- txnBegin txnDB2
                        txnPut txn6 k v4
                        txnGet txn6 txnDB2 k `shouldReturn` (Just v4)
                        txnDelete txn6 k
                        txnGet txn6 txnDB2 k `shouldReturn` Nothing
                        -- outside of txn6 the key still exists as v2
                        txnGet txn7 txnDB2 k `shouldReturn` (Just v2)

                        -- after commit it is gone for everyone
                        txnCommit txn6
                        txnGet txn7 txnDB2 k `shouldReturn` Nothing


            it "should support open, begin, put, get, getForUpdate, commit" $
                withSystemTempDirectory "rocksdb-txn1" $ \path -> do
                    withTxnDB path conf $ \txnDB -> do
                        let (k,v) = ("k","v")

                        -- write a key in a transaction
                        txn <- txnBegin txnDB
                        txnGet txn txnDB k `shouldReturn` Nothing
                        txnGetForUpdate txn txnDB k `shouldReturn` Nothing
                        txnPut txn k v
                        txnGet txn txnDB k `shouldReturn` (Just v)
                        txnGetForUpdate txn txnDB k `shouldReturn` (Just v)
                        txnCommit txn

                        -- now try reading the key again in another transaction
                        txn2 <- txnBegin txnDB
                        getResult2 <- txnGet txn2 txnDB k
                        txnGetForUpdate txn2 txnDB k `shouldReturn` (Just v)
                        txnCommit txn2
                        getResult2 `shouldBe` (Just v)

        describe "Database engine" $ do
            it "should store and delete items" $
                withSystemTempDirectory "rocksdb-delete" $ \path ->
                withTestDB path $ \db -> do
                    put db "zzz" "zzz"
                    delete db "zzz"
                    get db "zzz" `shouldReturn` Nothing
            it "should put items into the database and retrieve them" $
                withSystemTempDirectory "rocksdb1" $ \path ->
                withTestDB path $ \db -> do
                    put db "zzz" "zzz"
                    get db "zzz" `shouldReturn` Just "zzz"
            it "should store and retrieve items from different column families" $
                withSystemTempDirectory "rocksdbcf1" $ \path ->
                withTestDBCF path ["two"] $ \db -> do
                    let [two] = columnFamilies db
                    put db "one" "one"
                    get db "one" `shouldReturn` Just "one"
                    getCF db two "one" `shouldReturn` Nothing
                    putCF db two "two" "two"
                    getCF db two "two" `shouldReturn` Just "two"
                    get db "two" `shouldReturn` Nothing
        describe "Multiple concurrent threads" $ do
            it "should be able to put and retrieve items" $
                withSystemTempDirectory "rocksdb2" $ \path ->
                withTestDB path $ \db -> do
                    let str = cs . show
                        key i = "key" <> str i
                        val i = "val" <> str i
                        indices = [1..500] :: [Int]
                        keys = map key indices
                        vals = map val indices
                        kvs = zip keys vals
                    as1 <- mapM (\(k, v) -> async $ put db k v) kvs
                    mapM_ wait as1
                    as2 <- mapM (\k -> async $ get db k) keys
                    mapM wait as2 `shouldReturn` map Just vals

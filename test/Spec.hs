{-# LANGUAGE RankNTypes #-}
{- |
   Module      : Main
   Description : Tests for sorting
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Main (main) where

import Control.Quiver.Sort

import Control.Applicative      (liftA2)
import Control.Exception        (evaluate)
import Control.Monad            (void)
import Control.Monad.Catch      (throwM)
import Control.Quiver.Instances ()
import Control.Quiver.SP
import Data.Binary              (Binary)
import Data.Functor.Identity
import Data.List                (sort)
import System.Directory         (getDirectoryContents)
import System.IO.Temp           (withSystemTempDirectory)

import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck

--------------------------------------------------------------------------------

main :: IO ()
main = hspec $ do
  describe "in-memory" $
    prop "same as list-based" $
      forAllShrink (arbitrary :: Gen [Int]) shrink $
        liftA2 (==) sort (spIdentityList spsort)
  describe "file-based" $ do
    prop "same as list-based" $
      forAllShrink (arbitrary :: Gen [Int]) shrink $ \as (Positive cs) ->
        ioProperty $ (== sort as) <$> fileSort cs as
    describe "cleans up temporary files" $ do
      it "on success" $
        fileSortCleanup (spevery [1::Int .. 10])

      it "on exception" $
        fileSortCleanup (spevery [1::Int .. 10] >> throwM (userError "Tainted Producer"))

spToList :: SQ a x f [a]
spToList = spfoldr (:) []

spIdentity :: SQ a b Identity c -> c
spIdentity = runIdentity . sprun

spIdentityList :: SQ a b Identity e -> [a] -> [b]
spIdentityList p as = spIdentity (spList p as)

spList :: (Functor f) => P () a b () f e -> [a] -> Effect f [b]
spList p as = spevery as >->> p >->> spToList >&> snd

fileSort :: (Binary a, Ord a) => Int -> [a] -> IO [a]
fileSort cs as = sprun $ spList (spfilesort (Just cs) Nothing) as

-- The provided producer is assumed to be short.
fileSortCleanup :: (Show a, Binary a, Ord a) => Producer a () IO e -> Expectation
fileSortCleanup prod = withSystemTempDirectory "test-quiver-sort-cleanup" $ \tmpDir -> do
                         cnts0 <- getDirectoryContents tmpDir -- Should be [".", ".."]
                         _ <- sprun (pipeline tmpDir)
                         (sort <$> getDirectoryContents tmpDir) `shouldReturn` sort cnts0
  where
    pipeline tmpDir = prod
                      -- Use a chunk size of 1 to make sure files are
                      -- created, even if an exception is thrown.
                      >->> spfilesort (Just 1) (Just tmpDir) >&> snd
                      >->> sptraverse (void . evaluate) >&> fst -- Just to consume them all

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

import Control.Applicative   (liftA2)
import Control.Quiver.SP
import Data.Binary           (Binary)
import Data.Functor.Identity
import Data.List             (sort)

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
  describe "file-based" $
    prop "same as list-based" $
      forAllShrink (arbitrary :: Gen [Int]) shrink $ \as (Positive cs) ->
        ioProperty $ (== sort as) <$> fileSort cs as

spToList :: SQ a x f [a]
spToList = spfoldr (:) []

spIdentity :: SQ a b Identity c -> c
spIdentity = runIdentity . sprun

spIdentityList :: SQ a b Identity e -> [a] -> [b]
spIdentityList p as = spIdentity (spList p as)

spList :: (Functor f) => P () a b () f e -> [a] -> Effect f [b]
spList p as = spevery as >->> p >->> spToList >&> snd

fileSort :: (Binary a, Ord a) => Int -> [a] -> IO [a]
fileSort cs as = runEffect $ spList (spfilesort (Just cs) Nothing) as

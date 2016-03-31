{-# LANGUAGE RankNTypes #-}
{- |
   Module      : Control.Quiver.Sort
   Description : Sort values in a Quiver
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Control.Quiver.Sort (
    -- * In-memory sorting
    -- $memory
    spsort
  , spsortBy
  ) where

import Control.Monad     (ap)
import Control.Quiver.SP
import Data.Function     (on)
import Data.List         (sortBy)

--------------------------------------------------------------------------------

{- $memory

These Quivers require reading all the values from the quiver before
being able to sort them.  As such, it is /highly/ recommended you only
use them for short streams.

-}

spsort :: (Ord a, Monad m) => SP a a m ()
spsort = spsortBy compare

-- | Use the specified comparison function to sort the values.
spsortBy :: (Monad m) => (a -> a -> Ordering) -> SP a a m ()
spsortBy f = (sortBy f <$> spfoldr (:) []) >>= spevery

-- | Use the provided function to be able to compare values.
spsortOn :: (Ord b, Monad m) => (a -> b) -> SP a a m ()
spsortOn f = sppure (ap (,) f)
             >->> spsortBy (compare `on` snd)
             >->> sppure fst >&> snd

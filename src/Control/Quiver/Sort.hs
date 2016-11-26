{-# LANGUAGE PatternSynonyms, RankNTypes, ScopedTypeVariables, ViewPatterns #-}

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
  , spsortOn
    -- * File-based sorting
    -- $filesort
  , spfilesort
  , spfilesortBy
    -- ** Configuration
  , SPFileConfig
  , defaultConfig
  , setChunkSize
  , setTempDir
  , setMaxFiles
  ) where

import Control.Quiver.Binary
import Control.Quiver.ByteString
import Control.Quiver.Group
import Control.Quiver.Instances  ()
import Control.Quiver.Interleave
import Control.Quiver.SP

import           Control.Applicative          (liftA2)
import           Control.Exception            (IOException)
import           Control.Monad                (join)
import           Control.Monad.Catch          (MonadCatch(..), MonadMask,
                                               finally)
import           Control.Monad.IO.Class       (MonadIO(..))
import           Control.Monad.Trans.Resource (MonadResource, allocate)
import           Data.Bool                    (bool)
import           Data.Coerce                  (coerce)
import           Data.Foldable                (toList)
import           Data.Function                (on)
import           Data.List                    (sortBy)
import           Data.Monoid                  (First(..), (<>))
import           Data.Sequence                (Seq, (|>))
import qualified Data.Sequence                as S
import           System.Directory             (doesDirectoryExist,
                                               getPermissions,
                                               getTemporaryDirectory,
                                               removeDirectoryRecursive,
                                               removeFile, writable)
import           System.IO                    (hClose, openTempFile)
import           System.IO.Temp               (createTempDirectory)

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
spsortBy f = (sortBy f <$> spToList) >>= spevery

-- | Use the provided function to be able to compare values.
spsortOn :: (Ord b, Monad m) => (a -> b) -> SP a a m ()
spsortOn f = sppure ((,) <*> f)
             >->> spsortBy (compare `on` snd)
             >->> sppure fst >&> snd

spToList :: SQ a x f [a]
spToList = spfoldr (:) []

--------------------------------------------------------------------------------

{- $filesort

For large Quivers it may not be possible to sort the entire stream in
memory.  As such, these functions work by sorting chunks of the stream
and storing them in temporary files before merging them all together.

-}

-- | Configuration settings for 'spfilesort' and 'spfilesortBy'.  Use
-- 'defaultConfig' and the various @set*@ functions to configure it.
data SPFileConfig = FC { _chunkSize  :: !Int
                         -- ^ How large the chunks should be for
                         -- individual sorting.
                       , _withTmpDir :: !(Maybe FilePath)
                         -- ^ Where to store temporary files.  Will be
                         -- cleaned up afterwards.  'Nothing'
                         -- indicates to use the system temporary
                         -- directory.
                       , _maxFiles   :: !Int
                         -- ^ The maximum number of temporary files to
                         -- be open at any one time.
                       }

-- | Default settings for sorting using external files:
--
--   * Have a chunk size of @1000@.
--
--   * Use the system temporary directory.
--
--   * No more than @100@ temporary files to be open at a time.
defaultConfig :: SPFileConfig
defaultConfig = FC { _chunkSize  = 1000
                   , _withTmpDir = Nothing
                   , _maxFiles   = 100
                   }

-- | Specify the size of chunks to be individually sorted: the larger
-- the value the fewer temporary files need to be created but the more
-- memory needed to accumulate the values and sort them.
setChunkSize :: Int -> SPFileConfig -> SPFileConfig
setChunkSize cs cfg = cfg { _chunkSize = cs }

-- | Specify where temporary files should be stored.
--
--   Typically you would only set this if the system temporary
--   directory isn't large or fast enough.
--
--   NOTE: this directory /must/ exist and be writable!
setTempDir :: FilePath -> SPFileConfig -> SPFileConfig
setTempDir dir cfg = cfg { _withTmpDir = Just dir }

-- | The maximum number of files that should be open at any one time.
--
--   Larger values will be faster, but run the risk of exhausting the
--   operating system's supply of file descriptors (and thus being
--   killed).
setMaxFiles :: Int -> SPFileConfig -> SPFileConfig
setMaxFiles c cfg = cfg { _maxFiles = c }

-- | Use external files to temporarily store partially sorted results
-- (splitting into chunks of the specified size if one is provided).
--
-- These files are stored inside the specified directory if provided;
-- if no such directory is provided then the system temporary
-- directory is used.
spfilesort :: (Binary a, Ord a, MonadResource m, MonadMask m) => SPFileConfig
              -> P () a a () m (SPResult IOException)
spfilesort = spfilesortBy compare

-- | Use external files to temporarily store partially sorted (using
-- the comparison function) results (splitting into chunks of the
-- specified size if one is provided).
--
-- These files are stored inside the specified directory if provided;
-- if no such directory is provided then the system temporary
-- directory is used.
spfilesortBy :: (Binary a, MonadResource m, MonadMask m) => (a -> a -> Ordering) -> SPFileConfig
                -> P () a a () m (SPResult IOException)
spfilesortBy cmp cfg = do mdir' <- join <$> liftIO (traverse checkDir (_withTmpDir cfg))
                          getTmpDir mdir' "quiver-sort" pipeline
  where
    -- Make sure the directory exists and is writable.
    checkDir dir = do ex <- liftA2 (liftA2 (&&)) doesDirectoryExist (fmap writable . getPermissions) dir
                      return (bool Nothing (Just dir) ex)

    getTmpDir = maybe withSystemTempDirectory withTempDirectory

    pipeline tmpDir = toFiles tmpDir >>= either spfailed (sortFromFiles maxFiles cmp tmpDir)

    toFiles tmpDir = sortToFiles chunkSize cmp tmpDir >->> spToSeq >&> uncurry (flip checkFailed)

    chunkSize = _chunkSize cfg

    maxFiles = _maxFiles cfg

sortToFiles :: (Binary a, MonadIO m, MonadMask m) => Int -> (a -> a -> Ordering) -> FilePath
               -> SP a FilePath m IOException
sortToFiles chunkSize cmp tmpDir = spchunks chunkSize
                                   >->> spTraverseUntil sortChunk
                                   >&> snd
  where
    sortChunk as = writeOut tmpDir (spevery (sortBy cmp as))

writeOut :: (Binary a, MonadIO m, MonadMask m) => FilePath -> P () x a () m (SPResult IOException)
            -> m (Either IOException FilePath)
writeOut tmpDir p = do (fl,h) <- liftIO (openTempFile tmpDir "quiver-sort-chunk")
                       finally (checkFailed fl <$> sprun (pipeline h) <* liftIO (hClose h))
                               (liftIO (hClose h))
  where
    pipeline h = p >->> spencode >&> fst >->> qhoist liftIO (qPut h) >&> getFirstError

sortFromFiles :: (Binary a, MonadIO m, MonadMask m) => Int -> (a -> a -> Ordering) -> FilePath
                 -> Seq FilePath -> Producer a () m (SPResult IOException)
sortFromFiles mf cmp tmpDir = nextBatch
  where
    nextBatch Empty = spcomplete
    nextBatch fls   = case S.splitAt mf fls of
                        (b,Empty) -> batch b
                        (b,fls')  -> do br <- qlift (writeBatch b)
                                        liftIO $ mapM_ removeFile b
                                        either spfailed (nextBatch . (fls' |>)) br

    writeBatch = writeOut tmpDir . batch

    batch = spinterleave cmp . map readFl . toList

    -- Assume decoding is successful for now.
    readFl fl = qhoist liftIO (qReadFile fl readSize) >->> spdecode >&> fst

    readSize = 4096

-- Just to make it nicer to pattern-match
pattern Empty :: Seq a
pattern Empty <- (S.viewl -> S.EmptyL)

spTraverseUntil :: (Monad m) => (a -> m (Either e b)) -> SP a b m e
spTraverseUntil k = loop
  where
    loop = spconsume loop' spcomplete
    loop' a = qlift (k a) >>= either spfailed (>:> loop)

-- Ignore SPIncomplete values
checkFailed :: r -> SPResult e -> Either e r
checkFailed _ (Just (Just e)) = Left e
checkFailed r _               = Right r

spToSeq :: SQ a x f (Seq a)
spToSeq = spfoldl' (|>) mempty

--------------------------------------------------------------------------------
-- Creating the temporary directory

withSystemTempDirectory :: (MonadResource m) =>
                           String   -- ^ Directory name template. See 'openTempFile'.
                        -> (FilePath -> m a) -- ^ Callback that can use the directory
                        -> m a
withSystemTempDirectory template action = liftIO getTemporaryDirectory >>= \tmpDir -> withTempDirectory tmpDir template action

withTempDirectory :: (MonadResource m) =>
                     FilePath -- ^ Temp directory to create the directory in
                  -> String   -- ^ Directory name template. See 'openTempFile'.
                  -> (FilePath -> m a) -- ^ Callback that can use the directory
                  -> m a
withTempDirectory targetDir template withTmp = do
  (_release, tmpDir) <- allocate (createTempDirectory targetDir template)
                                 (ignoringIOErrors . removeDirectoryRecursive)
  withTmp tmpDir

ignoringIOErrors :: (MonadCatch m) => m () -> m ()
ignoringIOErrors ioe = ioe `catch` (\(_ :: IOError) -> return ())

getFirstError :: (SPResult a, SPResult a) -> SPResult a
getFirstError (r1,r2) = coerce (toFirst r1 <> toFirst r2)
  where
    toFirst :: SPResult a -> Maybe (First a)
    toFirst = coerce

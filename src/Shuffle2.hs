{-# LANGUAGE GADTs #-}
module Shuffle2 where
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Async.Pool
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan
-- import GHC.Conc (atomically)
import Control.Monad (replicateM)
import Control.Monad.Trans.Free
import Data.Foldable (toList)

import RDD
import Shuffle (localPartitionBy, partitionByLocal)

data ShuffleF x where
  Collect     :: Foldable f => RDD f a -> ([a] -> x) -> ShuffleF x
  PartitionBy :: Foldable f => (a -> Int) -> Int -> RDD f a -> (RDD [] a -> x) -> ShuffleF x

instance Functor ShuffleF where
  fmap f (Collect         r x) = Collect r (f . x)
  fmap f (PartitionBy g i r x) = PartitionBy g i r (f . x)

type Shuffle = FreeT ShuffleF

collect :: (Foldable f, Monad m) => RDD f a -> Shuffle m [a]
collect r = liftF $ Collect r id

partitionBy :: (Foldable f, Monad m) => (a -> Int) -> Int -> RDD f a -> Shuffle m (RDD [] a)
partitionBy f n r = liftF $ PartitionBy f n r id

example :: Monad m => Shuffle m [Int]
example = do
  r <- partitionBy id 3 $ produce [[1..20]]
  r' <- partitionBy id 4 $ fmap (+2) r
  collect r'

runShuffle :: Monad m => Shuffle m a -> m a
runShuffle = iterT go
  where go (Collect         r x) = x $ (fmap toList (extract r)) >>= id
        go (PartitionBy f i r x) = x $ localPartitionBy f i r

runAsyncShuffle :: Int -> Shuffle IO a -> IO a
runAsyncShuffle i r = withTaskGroup i go
  where go tg = iterT (run tg) r
        run tg (Collect         r x) = x $ (fmap toList (extract r)) >>= id
        run tg (PartitionBy f i r x) = x $ localPartitionBy f i r

asyncCollect :: Foldable f => RDD f a -> TaskGroup -> IO [a]
asyncCollect (RDD c n) tg = do
  r <- atomically . mapReduce tg $ map (return . toList . c) n
  evalMe tg r

asyncPartitionBy :: (a -> Int) -> Int -> RDD f a -> TaskGroup -> IO (RDD f a)
asyncPartitionBy = undefined

evalMe :: TaskGroup -> Async a -> IO a
evalMe tg as = Async.withAsync (runTaskGroup tg) $ \_ -> wait as

testCollect :: Int -> IO [Int]
testCollect i = do
  let rdd = produce [[1..10], [10..20], [20..30]]
  withTaskGroup i $ \tg -> do
    asyncCollect rdd tg

pullFromChan :: TaskGroup -> Int -> [TChan a] -> IO [[a]]
pullFromChan tg numP = runTask tg . sequenceA . map doRead
  where doRead chan = task . atomically $ replicateM numP (readTChan chan)

mapToChan :: [TChan a] -> [(Int, [a])] -> STM ()
mapToChan chans = sequence_ . map go
  where go (i, v) = sequence_ $ map (writeTChan (chans !! i)) v

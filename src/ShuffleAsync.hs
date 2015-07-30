module ShuffleAsync (runShuffle) where
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.Async.Pool (TaskGroup, mapReduce, runTaskGroup, withTaskGroup, Async, wait)
import Control.Concurrent.STM (atomically)
import Control.Monad.Trans.Free (iterT)
import Data.Foldable (toList)

import RDD
import Shuffle

runAndWait :: TaskGroup -> Async a -> IO a
runAndWait tg as = Async.withAsync (runTaskGroup tg) $ \_ -> wait as

asyncCollect :: TaskGroup -> RDD a -> IO [a]
asyncCollect tg (RDD c n) = do
  r <- atomically . mapReduce tg $ map (return . toList . c) n
  runAndWait tg r

asyncPartitionBy :: TaskGroup -> HashFunc a -> NumPartitions -> RDD a
                 -> IO (RDD a)
asyncPartitionBy tg f i r = do
  y <- asyncCollect tg $ keyByPartition f i r
  let new = fanout i y
  return $ RDD (new !!) [0..i - 1]

fanout :: Int -> [(Int, a)] -> [[a]]
fanout i = foldr go $ replicate i []
  where go :: (Int, a) -> [[a]] -> [[a]]
        go (i, v) x = map (meld i v) $ zip [0..] x
        meld i v (j, r) = if i == j then (v:r) else r

runShuffle :: Int -> Shuffle IO a -> IO a
runShuffle i s = withTaskGroup i $ \tg -> iterT (go tg) s
  where go tg (Collect         r x) = (>>= x) $ asyncCollect tg r
        go tg (PartitionBy f i r x) = (>>= x) $ asyncPartitionBy tg f i r

{-# LANGUAGE GADTs #-}
module ShuffleOperational where
import Control.Monad.Operational
import Data.Foldable (toList)
import Data.Functor.Identity
import Data.Hashable
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M

import RDD


data ShuffleI a where
  Collect     :: RDD a -> ShuffleI [a]
  PartitionBy :: HashFunc a -> NumPartitions -> RDD a -> ShuffleI (RDD a)

type Shuffle = ProgramT ShuffleI

collect :: RDD a -> Shuffle m [a]
collect = singleton . Collect

partitionBy :: (a -> Int) -> Int -> RDD a -> Shuffle m (RDD a)
partitionBy f n = singleton . PartitionBy f n

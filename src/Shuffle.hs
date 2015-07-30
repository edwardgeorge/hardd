{-# LANGUAGE GADTs #-}
module Shuffle where
import Control.Monad.Trans.Free (FreeT, liftF)
import Data.Hashable (Hashable, hash)
import Data.Monoid (Sum(..))

import RDD

data ShuffleF x where
  Collect     :: RDD a -> ([a] -> x) -> ShuffleF x
  PartitionBy :: HashFunc a -> NumPartitions -> RDD a -> (RDD a -> x)
              -> ShuffleF x

instance Functor ShuffleF where
  fmap f (Collect         r x) = Collect r (f . x)
  fmap f (PartitionBy g i r x) = PartitionBy g i r (f . x)

type Shuffle = FreeT ShuffleF

collect :: Monad m => RDD a -> Shuffle m [a]
collect r = liftF $ Collect r id

partitionBy :: Monad m =>
               HashFunc a -> NumPartitions -> RDD a
            -> Shuffle m (RDD a)
partitionBy f n r = liftF $ PartitionBy f n r id

repartition :: (Monad m, Hashable a) =>
               NumPartitions -> RDD a -> Shuffle m (RDD a)
repartition = partitionBy hash

foldMapRDD :: (Monad m, Monoid b) => (a -> b) -> RDD a -> Shuffle m b
foldMapRDD f r = do
  d <- collect $ foldMapLocally f r
  return $ mconcat d

count :: Monad m => RDD a -> Shuffle m Integer
count = fmap getSum . foldMapRDD (const $ Sum 1)

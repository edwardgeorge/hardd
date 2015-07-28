{-# LANGUAGE GADTs #-}
module Shuffle where
import Control.Monad.Trans.Free

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

foldMapRDD :: (Monad m, Monoid b) => (a -> b) -> RDD a -> Shuffle m b
foldMapRDD f r = do
  d <- collect $ foldMapLocally f r
  return $ mconcat d

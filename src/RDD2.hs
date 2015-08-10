{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
module RDD2 where
import Control.Monad (join, ap)
import Data.Functor.Identity

data RDD m w where
  RDD :: Traversable f => (s -> m (f a)) -> [s] -> RDD m a

type RDDi = RDD Identity

type KeyedRDD m k v = RDD m (k, v)

instance Functor m => Functor (RDD m) where
  fmap f (RDD c n) = RDD (fmap (fmap f) . c) n

bindRDD :: Monad m => (a -> m b) -> RDD m a -> RDD m b
bindRDD f (RDD c n) = RDD (go . c) n
  where go = join . fmap (mapM f)

apRDD :: Monad m => m (a -> b) -> RDD m a -> RDD m b
apRDD f (RDD c n) = RDD (go . c) n
  where go = join . fmap (sequence . fmap (ap f . return))

rdd :: Traversable f => (s -> f a) -> [s] -> RDDi a
rdd f = RDD (Identity . f)

mapPartitions :: (Functor m, Traversable g) =>
                 (forall f. Traversable f => f a -> g b)
              -> RDD m a -> RDD m b
mapPartitions f (RDD c n) = RDD (fmap f . c) n

mapPartitionsWithIndex :: (Functor m, Traversable g) =>
                          (forall f. Traversable f => Int -> f a -> g b)
                       -> RDD m a -> RDD m b
mapPartitionsWithIndex f (RDD c n) = RDD go [0..length n - 1]
  where go i = fmap (f i) . c $ n !! i

getNumPartitions :: RDD m a -> Int
getNumPartitions (RDD _ n) = length n

mapPartitionsM :: (Monad m, Traversable g) =>
                  (forall f. Traversable f => f a -> m (g b))
               -> RDD m a -> RDD m b
mapPartitionsM f (RDD c n) = RDD ((>>= f) . c) n

mapPartitionsWithIndexM :: (Monad m, Traversable g) =>
                           (forall f. Traversable f => Int -> f a -> m (g b))
                        -> RDD m a -> RDD m b
mapPartitionsWithIndexM f (RDD c n) = RDD go [0..length n - 1]
  where go i = (>>= f i) . c $ n !! i

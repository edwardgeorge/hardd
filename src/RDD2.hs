{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
module RDD2 where
import Data.Foldable (toList)

data RDD a where
  RDD :: Traversable f => (s -> f a) -> [s] -> RDD a

type KeyedRDD k v = RDD (k, v)

instance Functor RDD where
  fmap f (RDD c n) = RDD (fmap f . c) n

produce :: [[a]] -> RDD a
produce x = RDD (x !!) [0..length x - 1]

compute :: RDD a -> Int -> Maybe [a]
compute (RDD c n) i = fmap (toList . c) $ safeLookup n i
  where safeLookup []     _ = Nothing
        safeLookup (x:_)  0 = Just x
        safeLookup (_:xs) n = safeLookup xs (n - 1)

mapPartitions :: Traversable g => (forall f. Traversable f => f a -> g b) -> RDD a -> RDD b
mapPartitions f (RDD c n) = RDD (f . c) n

mapPartitionsWithIndex :: Traversable g => (forall f. Traversable f => Int -> f a -> g b) -> RDD a -> RDD b
mapPartitionsWithIndex f (RDD c n) = RDD go [0..length n - 1]
  where go i = f i . c $ n !! i

getNumPartitions :: RDD a -> Int
getNumPartitions (RDD _ n) = length n

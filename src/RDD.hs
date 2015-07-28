{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
module RDD where
import Data.Foldable (toList)
import Data.Functor.Identity
import Data.Monoid (Sum(..))
import qualified Data.Map.Strict as M

type HashFunc a = (a -> Int)
type NumPartitions = Int
type PartitionIndex = Int

data RDD a where
  RDD :: Traversable f => (s -> f a) -> [s] -> RDD a

type KeyedRDD k v = RDD (k, v)

instance Functor RDD where
  fmap f (RDD c n) = RDD (fmap f . c) n

produce :: [[a]] -> RDD a
produce x = RDD (x !!) [0..length x - 1]

produce' :: [a] -> RDD a
produce' x = RDD (const x) [()]

compute :: RDD a -> PartitionIndex -> Maybe [a]
compute (RDD c n) i = fmap (toList . c) $ safeLookup n i
  where safeLookup []     _ = Nothing
        safeLookup (x:_)  0 = Just x
        safeLookup (_:xs) n = safeLookup xs (n - 1)

mapPartitions :: Traversable g =>
                 (forall f. Traversable f => f a -> g b)
              -> RDD a -> RDD b
mapPartitions f (RDD c n) = RDD (f . c) n

mapPartitionsWithIndex :: Traversable g =>
                          (forall f. Traversable f => PartitionIndex -> f a -> g b)
                       -> RDD a -> RDD b
mapPartitionsWithIndex f (RDD c n) = RDD go [0..length n - 1]
  where go i = f i . c $ n !! i

getNumPartitions :: RDD a -> NumPartitions
getNumPartitions (RDD _ n) = length n

reduceByKeyLocally :: Ord k => (v -> v -> v) -> KeyedRDD k v -> KeyedRDD k v
reduceByKeyLocally f = mapPartitions (M.toList . go f)
  where go f = flip foldr M.empty $ \(k, v) -> M.insertWith f k v

keyByPartition :: HashFunc a -> NumPartitions -> RDD a -> KeyedRDD PartitionIndex a
keyByPartition f i = fmap $ \a -> (f a `mod` i, a)

foldMapLocally :: Monoid b => (a -> b) -> RDD a -> RDD b
foldMapLocally f = mapPartitions (Identity . foldMap f)

countLocally :: Num b => RDD a -> RDD b
countLocally = fmap getSum . foldMapLocally (const $ Sum 1)

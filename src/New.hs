{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
module New where
import Data.Foldable (toList)
import Data.Functor.Identity (Identity(..))
import Data.Hashable (Hashable)
import qualified Data.Map.Strict as M
import Data.Proxy (Proxy)
import Data.Traversable (fmapDefault)

import Join

type PartitionIndex = Int
type NumPartitions = PartitionIndex

type Keyed x k v = x (k, v)
type JoinKey a b = forall k. Hashable k => Either a b -> k
type HashFunc a = forall b. Hashable b => a -> b

class IsIndexable a where
  getPartitionIndex :: a -> PartitionIndex

class Local rdd x | x -> rdd where
  mapPartitionsWithIndex :: Traversable g
                         => (forall f s. (Traversable f, IsIndexable s) => s -> f a -> g b)
                         -> rdd a -> x (rdd b)

class Local rdd x => Shuffle rdd x | x -> rdd where
--  numPartitions :: rdd a -> x NumPartitions
  collectWith :: ([a] -> b) -> rdd a -> x b
  partitionBy :: (Hashable b => a -> b) -> NumPartitions -> rdd a -> x (rdd a)
  join        :: JoinKey a b -> Proxy (j :: JoinType)
              -> rdd a -> rdd b -> x (rdd (Joined j a b))

mapPartitions :: (Local rdd x, Traversable g) => (forall f. Traversable f => f a -> g b) -> rdd a -> x (rdd b)
mapPartitions f = mapPartitionsWithIndex (const f)

mapRDD :: Local rdd x => (a -> b) -> rdd a -> x (rdd b)
mapRDD f = mapPartitions (toList . fmapDefault f)

filterRDD :: Local rdd x => (a -> Bool) -> rdd a -> x (rdd a)
--filterRDD f = mapPartitions (filter f . toList)
filterRDD f = mapPartitions $ foldr go []
  where go a b = if f a then a:b else b

reduceByKeyLocal :: (Local rdd x, Ord k) => (v -> v -> v) -> Keyed rdd k v -> x (Keyed rdd k v)
reduceByKeyLocal f = mapPartitions (M.toList . go f)
  where go f = flip foldr M.empty $ \(k, v) -> M.insertWith f k v

reduceByKey :: (Shuffle rdd x, Monad x, Ord k, Hashable k)
            => (v -> v -> v) -> NumPartitions -> Keyed rdd k v -> x (Keyed rdd k v)
reduceByKey f p r = do x <- reduceByKeyLocal f r
                       y <- partitionBy fst p x
                       reduceByKeyLocal f y

reduceLocal :: Local rdd x => (a -> b -> b) -> b -> rdd a -> x (rdd b)
reduceLocal ma me = mapPartitions $ Identity . foldr ma me

reduce :: (Monad x, Shuffle rdd x) => (a -> b) -> (b -> b -> b) -> b -> rdd a -> x b
reduce f g e r = do x <- reduceLocal (g . f) e r
                    collectWith (foldr g e) x

-- -------

mapValues :: Local rdd x => (a -> b) -> Keyed rdd k a -> x (Keyed rdd k b)
mapValues f = mapRDD $ fmap f

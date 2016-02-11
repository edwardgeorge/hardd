{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
module New where
import Data.Constraint (Dict(Dict))
import Data.Foldable (toList)
import Data.Functor.Identity (Identity(..))
import Data.Hashable (Hashable)
import qualified Data.Map.Strict as M
import qualified Data.Sequence as S
import Data.Traversable (fmapDefault)

import Join

type PartitionIndex = Int
type NumPartitions = PartitionIndex

type Keyed x k v = x (k, v)
type JoinKey a b = forall k. Hashable k => Either a b -> k
type HashFunc a b = (Hashable b => a -> b)

class IsIndexable a where
  getPartitionIndex :: a -> PartitionIndex

class Local rdd x | x -> rdd where
  -- mapPartitionsWithIndex (const id) == return
  mapPartitionsWithIndex :: Traversable g
                         => (forall f s. (Traversable f, IsIndexable s) => s -> f a -> g b)
                         -> rdd a -> x (rdd b)

class Local rdd x => Shuffle rdd x | x -> rdd where
--  numPartitions :: rdd a -> x NumPartitions
  collectWith :: ([a] -> b) -> rdd a -> x b
  partitionBy :: HashFunc a b -> NumPartitions -> rdd a -> x (rdd a)
  join        :: JoinKey a b -> proxy (j :: JoinType)
              -> rdd a -> rdd b -> x (rdd (Joined j a b))
  union       :: rdd a -> rdd a -> x (rdd a)

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

groupByKey :: (Shuffle rdd x, Monad x, Ord k, Hashable k) => NumPartitions -> Keyed rdd k v -> x (Keyed rdd k [v])
groupByKey n r = do x <- mapValues (:[]) r
                    reduceByKey (++) n x

reduceByKeyLocal' :: (Local rdd x, Ord k) => (a -> b -> b) -> b -> Keyed rdd k a -> x (Keyed rdd k b)
reduceByKeyLocal' f b = mapPartitions (M.toList . go f)
  where go g = flip foldr M.empty $ \(k, v) -> M.alter (upd v g) k
        upd v g (Just a) = Just $ g v a
        upd v g Nothing  = Just $ g v b

reduceByKey' :: (Shuffle rdd x, Monad x, Ord k, Hashable k)
             => (a -> b -> b) -> (b -> b -> b) -> b -> NumPartitions -> Keyed rdd k a -> x (Keyed rdd k b)
reduceByKey' f g e n r = do x <- reduceByKeyLocal' f e r
                            y <- partitionBy fst n x
                            reduceByKeyLocal g y

groupByKey' :: (Shuffle rdd x, Monad x, Ord k, Hashable k) => NumPartitions -> Keyed rdd k v -> x (Keyed rdd k [v])
groupByKey' = reduceByKey' (:) (++) []

groupByKeySeq :: (Shuffle rdd x, Monad x, Ord k, Hashable k) => NumPartitions -> Keyed rdd k v -> x (Keyed rdd k (S.Seq v))
groupByKeySeq = reduceByKey' (S.<|) (S.><) (S.empty)

--toHashFunc :: Hashable b => (a -> b) -> a -> Dict Hashable
--toHashFunc f = undefined

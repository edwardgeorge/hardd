{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
module New where
import Data.Foldable (toList)
import Data.Functor.Identity (Identity(..))
import Data.Hashable (Hashable)  -- from: hashable
import qualified Data.Map.Strict as M  -- from: containers
import qualified Data.Sequence as S  -- from: containers
import Data.Traversable (fmapDefault)
import GHC.TypeLits

import Exists
import Join

type PartitionIndex = Int

type Keyed x k v = x (k, v)
type JoinKey a b = Either a b -> Exists Hashable
type HashFunc a = a -> Exists Hashable

type PartitionMap a b = forall f. Traversable f => f a -> ExistsF Traversable b
type IndexedPartitionMap a b = forall s. IsIndexable s => s -> PartitionMap a b

class IsIndexable a where
  getPartitionIndex :: a -> PartitionIndex

class Shuffle (rdd :: Nat -> * -> *) (x :: * -> *) | x -> rdd where
--  numPartitions :: rdd a -> x NumPartitions
  -- mapPartitionsWithIndex (const id) == return
  mapPartitionsWithIndex :: IndexedPartitionMap a b -> rdd n a -> x (rdd n b)
  collectWith            :: ([a] -> b) -> rdd n a -> x b
  partitionBy            :: HashFunc a -> numPartitions (n :: Nat) -> rdd m a -> x (rdd n a)
  join                   :: JoinKey a b -> proxy (j :: JoinType)
                         -> rdd n a -> rdd m b -> x (rdd (n * m) (Joined j a b))
  union                  :: rdd n a -> rdd m a -> x (rdd (n + m) a)

mapPartitions :: Shuffle rdd x => PartitionMap a b -> rdd n a -> x (rdd n b)
mapPartitions f = mapPartitionsWithIndex (\s a -> f a)

mapRDD :: Shuffle rdd x => (a -> b) -> rdd n a -> x (rdd n b)
mapRDD f = mapPartitions (\a k -> k $ fmapDefault f a)

filterRDD :: Shuffle rdd x => (a -> Bool) -> rdd n a -> x (rdd n a)
--filterRDD f = mapPartitions (filter f . toList)
filterRDD f = mapPartitions $ \a k -> k $ foldr go [] a
  where go a b = if f a then a:b else b

reduceByKeyLocal :: (Shuffle rdd x, Ord k) => (v -> v -> v) -> Keyed (rdd n) k v -> x (Keyed (rdd n) k v)
reduceByKeyLocal f = mapPartitions (\a k -> k . M.toList $ go f a)
  where go f = flip foldr M.empty $ \(k, v) -> M.insertWith f k v


reduceByKey :: (Shuffle rdd x, Monad x, Ord k, Hashable k)
            => (v -> v -> v) -> proxy (m :: Nat) -> Keyed (rdd n) k v -> x (Keyed (rdd m) k v)
reduceByKey f p r = do x <- reduceByKeyLocal f r
                       y <- partitionBy keyToHash p x
                       reduceByKeyLocal f y

reduceLocal :: Shuffle rdd x => (a -> b -> b) -> b -> rdd n a -> x (rdd n b)
reduceLocal ma me = mapPartitions $ \a k -> k . Identity $ foldr ma me a

reduce :: (Monad x, Shuffle rdd x) => (a -> b) -> (b -> b -> b) -> b -> rdd n a -> x b
reduce f g e r = do x <- reduceLocal (g . f) e r
                    collectWith (foldr g e) x
-- -------

mapValues :: Shuffle rdd x => (a -> b) -> Keyed (rdd n) k a -> x (Keyed (rdd n) k b)
mapValues f = mapRDD $ fmap f

groupByKey :: (Shuffle rdd x, Monad x, Ord k, Hashable k) => proxy (m :: Nat)
           -> Keyed (rdd n) k v -> x (Keyed (rdd m) k [v])
groupByKey n r = do x <- mapValues (:[]) r
                    reduceByKey (++) n x

reduceByKeyLocal' :: (Shuffle rdd x, Ord k) => (a -> b -> b) -> b -> Keyed (rdd n) k a -> x (Keyed (rdd n) k b)
reduceByKeyLocal' f b = mapPartitions (\a k -> k . M.toList $ go f a)
  where go g = flip foldr M.empty $ \(k, v) -> M.alter (upd v g) k
        upd v g (Just a) = Just $ g v a
        upd v g Nothing  = Just $ g v b

reduceByKey' :: (Shuffle rdd x, Monad x, Ord k, Hashable k)
             => (a -> b -> b) -> (b -> b -> b) -> b -> proxy (m :: Nat)
             -> Keyed (rdd n) k a -> x (Keyed (rdd m) k b)
reduceByKey' f g e n r = do x <- reduceByKeyLocal' f e r
                            y <- partitionBy keyToHash n x
                            reduceByKeyLocal g y

groupByKey' :: (Shuffle rdd x, Monad x, Ord k, Hashable k) => proxy (m :: Nat)
            -> Keyed (rdd n) k v -> x (Keyed (rdd m) k [v])
groupByKey' = reduceByKey' (:) (++) []

groupByKeySeq :: (Shuffle rdd x, Monad x, Ord k, Hashable k) => proxy (m :: Nat)
              -> Keyed (rdd n) k v -> x (Keyed (rdd m) k (S.Seq v))
groupByKeySeq = reduceByKey' (S.<|) (S.><) (S.empty)


toHashFunc :: Hashable b => (a -> b) -> HashFunc a
toHashFunc f a g = g (f a)

keyToHash :: Hashable k => HashFunc (k, v)
keyToHash = toHashFunc fst

toJoinKey :: Hashable k => (Either a b -> k) -> JoinKey a b
toJoinKey f e g = g (f e)

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
module Class where
import Data.Hashable (Hashable)  -- from: hashable
import GHC.TypeLits

import Exists
import Join

type PartitionIndex = Int

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

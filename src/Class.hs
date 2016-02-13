{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
module Class where
import Data.Hashable (Hashable)  -- from: hashable
import Data.Proxy
import GHC.TypeLits

import Exists
import Join

type PartitionIndex = Int

type JoinKey a b = Either a b -> Exists Hashable
type HashFunc a = a -> Exists Hashable

-- type NumPartitions n = forall proxy. KnownNat n => proxy (n :: Nat)
--data NumPartitions n = forall proxy. KnownNat n => NumPartitions (proxy n)
data NumPartitions (n :: Nat) where
  NumPartitions :: KnownNat n => proxy n -> NumPartitions n

numPartitions :: KnownNat n => NumPartitions n
numPartitions = NumPartitions Proxy

type PartitionMap a b = forall f. Traversable f => f a -> ExistsF Traversable b
type IndexedPartitionMap a b = forall s. IsIndexable s => s -> PartitionMap a b

-- ugh, need to pattern patch to bring the KnownNat constraint into scope
natValP :: NumPartitions n -> Integer
natValP (NumPartitions n) = natVal n

class IsIndexable a where
  getPartitionIndex :: a -> PartitionIndex

class Shuffle (rdd :: Nat -> * -> *) (x :: * -> *) | x -> rdd where
--  numPartitions :: rdd a -> x NumPartitions
  -- mapPartitionsWithIndex (const id) == return
  mapPartitionsWithIndex :: IndexedPartitionMap a b -> rdd n a -> x (rdd n b)
  collectWith            :: ([a] -> b) -> rdd n a -> x b
  partitionBy            :: HashFunc a -> NumPartitions n -> rdd m a -> x (rdd n a)
  joinRDDs               :: JoinKey a b -> proxy (j :: JoinType)
                         -> rdd n a -> rdd m b -> x (rdd (n * m) (Joined j a b))
  unionRDDs              :: rdd n a -> rdd m a -> x (rdd (n + m) a)

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
module RDD where
import GHC.TypeLits

import Class
import Exists

data RDD (n :: Nat) a where
  RDD :: IsIndexable s => (s -> ExistsF Traversable a) -> [s] -> RDD n a

liftIndexMap :: IndexedPartitionMap a b -> RDD n a -> RDD n b
liftIndexMap f (RDD g n) = RDD (\s -> g s (f s)) n

liftMap :: PartitionMap a b -> RDD n a -> RDD n b
liftMap f (RDD g n) = RDD (\s -> g s f) n

{-# LANGUAGE GADTs #-}
module Shuffle where
import Control.Monad.Operational
import Data.Foldable (toList)
import Data.Functor.Identity
import Data.Hashable
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M

import RDD


data ShuffleI a where
  Collect     :: Foldable f => RDD f a -> ShuffleI [a]
  PartitionBy :: Foldable f => (a -> Int) -> Int -> RDD f a -> ShuffleI (RDD [] a)

type Shuffle = ProgramT ShuffleI

collect :: Foldable f => RDD f a -> Shuffle m [a]
collect = singleton . Collect

partitionBy :: Foldable f => (a -> Int) -> Int -> RDD f a -> Shuffle m (RDD [] a)
partitionBy f n = singleton . PartitionBy f n

example :: Monad m => Shuffle m [Int]
example = do
  r <- partitionBy id 3 $ produce [[1..20]]
  collect r

example' :: Monad m => Shuffle m [Int]
example' = partitionBy id 3 (produce [[1..2]]) >>= collect

runShuffle :: Monad m => Shuffle m a -> m a
runShuffle x = viewT x >>= eval
  where eval (Return a)                         = return a
        eval (Collect         r :>>= k) = runShuffle . k $ (fmap toList (extract r)) >>= id
        eval (PartitionBy f i r :>>= k) = runShuffle . k $ localPartitionBy f i r

zipWithIndex :: (Monad m, Foldable f) => RDD f a -> Shuffle m (KeyedRDD f Integer a)
zipWithIndex r@(RDD c n) = do
  let sum = Identity . foldr (const (+ 1)) 0
  d <- collect $ mapPartitions sum r
  return $ undefined

reduce :: (Monad m, Foldable f) => (a -> a -> a) -> RDD f a -> Shuffle m a
reduce f r = do
  d <- collect $ mapPartitions (Identity . foldr1 f) r
  return $ foldr1 f d

reduceByKey :: (Monad m, Foldable f, Hashable b, Ord b) =>
               (a -> a -> a) -> KeyedRDD f b a -> Shuffle m (KeyedRDD [] b a)
reduceByKey f r = reduceByKey' f (getNumPartitions r) r

reduceByKey' :: (Monad m, Foldable f, Hashable b, Ord b) =>
                (a -> a -> a) -> Int -> KeyedRDD f b a -> Shuffle m (KeyedRDD [] b a)
reduceByKey' f i r = do
  r' <- partitionBy (hash . fst) i $ reduceByKeyLocally f r
  return $ reduceByKeyLocally f r'

reduceByKeyLocally :: (Foldable f, Ord b) => (a -> a -> a) -> KeyedRDD f b a -> KeyedRDD [] b a
reduceByKeyLocally f = mapPartitions (M.toList . go)
  where go = flip foldr M.empty $ \(k, v) -> M.insertWith f k v

example2 :: Monad m => Shuffle m Int
example2 = do
  r <- partitionBy id 3 $ produce [[1..20]]
  reduce (+) r

partitionByLocal :: Foldable f => (a -> Int) -> Int -> RDD f a -> RDD [] (Int, [a])
partitionByLocal f i = mapPartitions (IM.toList . go)
  where go = flip foldr IM.empty $ \a -> IM.insertWith (++) (f a `mod` i) [a]

flarn :: Foldable f => (a -> Int) -> Int -> f a -> IM.IntMap [a]
flarn f i = flip foldr IM.empty $ \a -> IM.insertWith (++) (f a `mod` i) [a]

blarn :: Foldable f => (a -> Int) -> Int -> [f a] -> IM.IntMap [a]
blarn f i = flip foldr IM.empty $ \a -> let m = flarn f i a
                                        in IM.unionWith (++) m

localPartitionBy :: Foldable f => (a -> Int) -> Int -> RDD f a -> RDD [] a
localPartitionBy f i (RDD c n) = let mm = go $ map c n
                       in RDD (maybe [] id . flip IM.lookup mm) [0..i-1]
  where go = flip foldr IM.empty $ \a -> IM.unionWith (++) $ flarn f i a

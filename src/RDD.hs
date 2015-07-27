{-# LANGUAGE GADTs #-}
module RDD where

data RDD f a where
  RDD :: (s -> f a) -> [s] -> RDD f a

type KeyedRDD f k v = RDD f (k, v)

extract :: RDD f a -> [f a]
extract (RDD c n) = map c n

produce :: [f a] -> RDD f a
produce = RDD id

compute :: RDD f a -> Int -> Maybe (f a)
compute (RDD c n) i = fmap c $ n `safeLookup` i
  where safeLookup []     _ = Nothing
        safeLookup (x:_)  0 = Just x
        safeLookup (_:xs) n = safeLookup xs (n - 1)

mapPartitions :: (f a -> g b) -> RDD f a -> RDD g b
mapPartitions f (RDD c n) = RDD (f . c) n

mapPartitionsWithIndex :: (Int -> f a -> g b) -> RDD f a -> RDD g b
mapPartitionsWithIndex f (RDD c n) = RDD go [0..length n - 1]
  where go i = f i . c $ n !! i

getNumPartitions :: RDD f a -> Int
getNumPartitions (RDD _ n) = length n

instance Functor f => Functor (RDD f) where
  fmap f (RDD c n) = RDD (fmap f . c) n

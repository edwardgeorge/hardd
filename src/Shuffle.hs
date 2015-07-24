{-# LANGUAGE GADTs #-}
module Shuffle where
import Control.Monad (join)
import Control.Monad.Free
import Control.Monad.Operational
import Control.Monad.State
import Data.Foldable (toList)
import Data.Functor.Identity
import Data.Hashable
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes)

data RDD f a where
  RDD :: (s -> f a) -> [s] -> RDD f a

extract :: RDD f a -> [f a]
extract (RDD c n) = map c n

produce :: [f a] -> RDD f a
produce = RDD id

compute :: RDD f a -> Int -> Maybe (f a)
compute (RDD c n) i = go n i
  where go [] _ = Nothing
        go (x:xs) 0 = Just $ c x
        go (x:xs) j = go xs $ j - 1

mapPartitions :: (f a -> g b) -> RDD f a -> RDD g b
mapPartitions f (RDD c n) = RDD (f . c) n

getNumPartitions :: RDD f a -> Int
getNumPartitions (RDD _ n) = length n

--cataPartitions :: (f a -> b) -> RDD f a -> RDD Identity b
--cataPartitions f (RDD c n) = RDD (Identity . f . c) n

type FileName = String
type LineData = String
type HashFunc a = (a -> Integer)
type KeyedRDD f k v = RDD f (k, v)
--type MapRDD k v = RDD (M.Map k) v

-- class MonadShuffle m where
--   --load :: [FileName] -> m (RDD [] LineData)
--   partitionBy :: Integer -> HashFunc a -> RDD f a -> m (RDD f a)
--   collect :: Foldable f => RDD f a -> m [a]
--   foldMapRDD :: (Monoid b, Foldable f) => (a -> b) -> RDD f a -> m b
--   --foldRDD :: Foldable f => (a -> b) -> (b -> b -> b) -> RDD f a -> m (Maybe b)
--   foldrRDD :: Foldable f => (a -> b) -> (b -> b -> b) -> b -> RDD f a -> m b
--
-- instance Functor f => Functor (RDD f) where
--   fmap f (RDD c n) = RDD (fmap f . c) n
--
-- instance Foldable f => Foldable (RDD f) where
--   foldMap f (RDD c n) = mconcat $ map (foldMap f . c) n
--
-- instance Traversable f => Traversable (RDD f) where
--   sequenceA = fmap produce . sequenceA . fmap sequenceA . extract
--
-- instance MonadShuffle Identity where
--   partitionBy i hf r = undefined
--   collect = Identity . join . map toList . extract
--   foldMapRDD f = Identity . mconcat . map runIdentity . extract . mapPartitions (Identity . foldMap f)
-- --  foldRDD f g = Identity . foldr1 g . catMaybes . extract . mapPartitions (foldr go Nothing)
-- --    where go a Nothing  = Just $ f a
-- --          go a (Just b) = Just $ g (f a) b

-- data ShuffleF x = Shuffle x
--                 | Collect (RDD f a)
--                   deriving (Show)
--
-- instance Functor ShuffleF where
--   fmap f (Shuffle x) = Shuffle (f x)
--   fmap f (Collect x) = Shuffle (f x)
--
-- type Shuffle = Free ShuffleF
--
-- shuffle :: RDD f a -> Shuffle (RDD f a)
-- shuffle r = liftF $ undefined
--
-- collect :: Foldable f => RDD f a -> Shuffle [a]
-- collect r = liftF $ Collect r

-- collect :: RDD f a -> [a]
-- partitionBy :: (a -> Int) -> Int -> RDD f a -> RDD f a

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

foo :: Foldable f => (a -> Int) -> Int -> f a -> State (IM.IntMap [a]) ()
foo f i = mapM_ go
  where go a = modify $ IM.insertWith (++) (f a `mod` i) [a]

bar :: Foldable f => (a -> Int) -> Int -> RDD f a -> State (IM.IntMap [a]) ()
bar f i (RDD c n) = sequence_ $ map (foo f i . c) n


-- foo :: (a -> Int) -> RDD f a -> State (IM.IntMap [a]) ()
-- foo toInt (RDD c n) = sequence_ $ map (go .) n
--   where go fa = mapM doMod fa
--         doMod a = modify $ IM.insertWith (++) (toInt a) [a]

-- localPartitionBy' :: Foldable f => (a -> Int) -> Int -> RDD f a -> RDD [] a
-- localPartitionBy' f i r = let m = map (go . c) n
--                           in RDD (m IM.!!) [0..i-1]
--   where go xs = foldr

-- zipWithIndex :: RDD f a -> KeyedRDD f Integer a
-- zipWithIndex (RDD c n) = RDD foo [0..length n - 1]
--   where go i = mapAccumL acc i
--     acc a b = (a + 1, (a, b))

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

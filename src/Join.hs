{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
module Join where
import Data.Bifunctor
-- import Data.Semigroup

type Join' a b = Either (Either a b) (a, b)

data JoinType = LeftOuterJoin | RightOuterJoin | FullOuterJoin | InnerJoin

class Join (x :: JoinType) where
  type Joined x (l :: *) (r :: *) :: *
  joinOn :: proxy x -> Join' a b -> Maybe (Joined x a b)

instance Join 'LeftOuterJoin where
  type Joined 'LeftOuterJoin l r = (l, Maybe r)
  joinOn _ (Left (Left  a)) = Just (a, Nothing)
  joinOn _ (Left (Right _)) = Nothing
  joinOn _ (Right   (a, b)) = Just (a, Just b)

instance Join 'RightOuterJoin where
  type Joined 'RightOuterJoin l r = (Maybe l, r)
  joinOn _ (Left (Left  _)) = Nothing
  joinOn _ (Left (Right b)) = Just (Nothing, b)
  joinOn _ (Right   (a, b)) = Just (Just a, b)

instance Join 'FullOuterJoin where
  type Joined 'FullOuterJoin l r = Join' l r
  joinOn _ x = Just x

instance Join 'InnerJoin where
  type Joined 'InnerJoin l r = (l, r)
  joinOn _ (Left (Left  _)) = Nothing
  joinOn _ (Left (Right _)) = Nothing
  joinOn _ (Right       x ) = Just x

newtype JoinVal a b = JoinVal (Join' a b)
                    deriving (Eq, Show)

instance Functor (JoinVal a) where
  fmap f (JoinVal r) = JoinVal $ bimap (fmap f) (fmap f) r

instance Bifunctor JoinVal where
  bimap f g (JoinVal r) = JoinVal $ bimap (bimap f g) (bimap f g) r

-- instance Semigroup a => Applicative (JoinVal a) where
--   pure a = JoinVal . Left $ Right a
--   JoinVal (Left (Left  a)) <*> JoinVal (Left (Left  a')) = JoinVal (Left (Left $ a <> a'))
--   JoinVal (Left (Left  a)) <*> JoinVal (Left (Right _ )) = JoinVal (Left (Left a))
--   JoinVal (Left (Left  a)) <*> JoinVal (Right  (a', _ )) = JoinVal (Left (Left $ a <> a'))
--   JoinVal (Left (Right _)) <*> JoinVal (Left (Left  a')) = JoinVal (Left (Left a'))
--   JoinVal (Left (Right b)) <*> JoinVal (Left (Right b')) = JoinVal (Left (Right $ b b'))
--   JoinVal (Left (Right b)) <*> JoinVal (Right  (a', b')) = JoinVal (Right (a', b b'))
--   JoinVal (Right   (a, _)) <*> JoinVal (Left (Left  a')) = JoinVal (Left (Left $ a <> a'))
--   JoinVal (Right   (a, b)) <*> JoinVal (Left (Right b')) = JoinVal (Right (a, b b'))
--   JoinVal (Right   (a, b)) <*> JoinVal (Right  (a', b')) = JoinVal (Right (a <> a', b b'))

-- instance Semigroup a => Monad (JoinVal a) where
--   --return a = JoinVal . Left $ Right a
--   return = pure
--   JoinVal (Left (Left  a)) >>= _ = JoinVal . Left $ Left a
--   JoinVal (Left (Right b)) >>= f = f b
--   JoinVal (Right   (a, b)) >>= f = case f b of
--     JoinVal (Left (Left  a')) -> JoinVal (Left (Left $ a <> a'))
--     JoinVal (Left (Right b')) -> JoinVal (Right (a, b'))
--     JoinVal (Right  (a', b')) -> JoinVal (Right (a <> a', b'))

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
module Join where
import Data.Proxy (Proxy)

type Join' a b = Either (Either a b) (a, b)

data JoinType = LeftOuterJoin | RightOuterJoin | FullOuterJoin | InnerJoin

class Join (x :: JoinType) where
  type Joined x (l :: *) (r :: *) :: *
  --checkJoin :: Proxy x -> Join' a b -> Bool
  joinOn :: Proxy x -> Join' a b -> Maybe (Joined x a b)

instance Join 'LeftOuterJoin where
  type Joined 'LeftOuterJoin l r = (l, Maybe r)
--  checkJoin _ (Left (Left  _)) = True
--  checkJoin _ (Left (Right _)) = False
--  checkJoin _ (Right       _ ) = True
  joinOn _ (Left (Left  a)) = Just (a, Nothing)
  joinOn _ (Left (Right _)) = Nothing
  joinOn _ (Right   (a, b)) = Just (a, Just b)

instance Join 'RightOuterJoin where
  type Joined 'RightOuterJoin l r = (Maybe l, r)
--  checkJoin _ (Left (Left  _)) = False
--  checkJoin _ (Left (Right _)) = True
--  checkJoin _ (Right       _ ) = True
  joinOn _ (Left (Left  _)) = Nothing
  joinOn _ (Left (Right b)) = Just (Nothing, b)
  joinOn _ (Right   (a, b)) = Just (Just a, b)

instance Join 'InnerJoin where
  type Joined 'InnerJoin l r = (l, r)
--  checkJoin _ (Left (Left  _)) = False
--  checkJoin _ (Left (Right _)) = False
--  checkJoin _ (Right       _ ) = True
  joinOn _ (Left (Left  _)) = Nothing
  joinOn _ (Left (Right _)) = Nothing
  joinOn _ (Right       x ) = Just x

instance Join 'FullOuterJoin where
  type Joined 'FullOuterJoin l r = Join' l r
  joinOn _ x = Just x

{-# LANGUAGE ConstraintKinds, RankNTypes #-}
module Exists where

type Exists c = forall r. (forall a. c a => a -> r) -> r
type ExistsF c a = forall r. (forall f. c f => f a -> r) -> r

{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveDataTypeable #-}

-- |
-- http://www.cs.cmu.edu/~dga/papers/zhou-sea2013.pdf

module Data.Vector.Succinct.Poppy where

import Data.Bits
import Data.Vector.Array
import Control.Lens as L
import Data.Vector.Bit hiding (rank)
import Data.Word
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Internal.Check as Ck
import Prelude hiding (null)

#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

class SucBV sbv where
    isobv :: Iso' sbv (Array Bit)
    rank :: Bit -> Int -> sbv -> Int
    select :: Bit -> Int -> sbv -> Int

data Poppy = Poppy {-# UNPACK #-} !Int !(Array Bit) 
                !(U.Vector Word64) 
                !(U.Vector Word64)

_Poppy :: Iso' Poppy (Array Bit)
_Poppy = iso (\(Poppy _ v _ _) -> v) $ f
  where f v@(V_Bit n ws) = let (l,l') = buildPoppy ws in Poppy n v l l'
-- Poppy
--
-- blocks                          indices
-- basic block size = 2^9 bits     3/4*10 bits par 2^9 bits
-- lower block size = 2^11 bits    32 bits par 2^11 bits
-- upper block size = 2^32 bits    64 bits par 2^32 bits
--                                 3.12500149% additional space
--



buildPoppy :: U.Vector Word64 -> (U.Vector Word64, U.Vector Word64)
buildPoppy ws = undefined -- todo
 


-- |
-- >>> search (\n->n^2 > 16) 0 100
search :: Integral n => (n -> Bool) -> n -> n -> n
search p l h | l == h = l
             | p m = search p l m
             | otherwise = search p (m+1) h
             where m = l `div` 2 + h `div` 2

-- CSPoppy supporting select0 and select1
--
-- sampling width = 8192           32 bits par min 2^13 bits
--                                 3.12500 + max 0.390625% additional space

data CSPoppy = CSPoppy {-# UNPACK #-} !Int !(Array Bit) 
                  !(U.Vector Word64) 
                  !(U.Vector Word64)
                  !(U.Vector Word32)
                  !(U.Vector Word32)

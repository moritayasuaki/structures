{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Data.Vector.Succinct.WaveletMatrix where

import Data.Vector.Succinct.Poppy
import Data.Vector.Array
import Data.Vector.Bit 
import qualified Data.Vector.Generic as G
import Data.Bits
import Control.Lens
import Data.Vector as B
import Data.Vector.Unboxed as U

newtype WaveletMatrix = WaveletMatrix (B.Vector CSPoppy) deriving Show

-- |
-- >>> buildWM (U.fromList [1,25,123,51,51])

buildWM :: U.Vector Int -> WaveletMatrix
buildWM bits = WaveletMatrix (B.fromList (butterfly 64 bits))

butterfly :: Int -> U.Vector Int -> [CSPoppy]
butterfly 0 v = []
butterfly i v = sucv : butterfly (i-1) (v0 U.++ v1) 
    where v0 = U.filter (not . flip testBit (i-1)) v
          v1 = U.filter (flip testBit (i-1)) v
          sucv = _CSPoppy # U.map (Bit . flip testBit (i-1)) v


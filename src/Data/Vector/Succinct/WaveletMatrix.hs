{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Data.Vector.Succinct.WaveletMatrix where

import Data.Vector.Succinct.Poppy as P
import Data.Vector.Array
import Data.Vector.Bit 
import qualified Data.Vector.Generic as G
import Data.Bits 
import Control.Lens
import Data.Vector as B
import Data.Vector.Unboxed as U

newtype WaveletMatrix = WaveletMatrix (B.Vector CSPoppy) deriving Show

-- |
-- >>> wmSelect 0 1 $ buildWM (U.fromList [3,4,0,0,7,6,1,2,2,0,1,6,5])
-- 3

buildWM :: U.Vector Int -> WaveletMatrix
buildWM bits = WaveletMatrix (B.fromList (Prelude.map (_CSPoppy #) (wave 64 bits)))
{-# INLINE buildWM #-}

wave :: Int -> U.Vector Int -> [U.Vector Bit]
wave 0 v = []
wave l v = bv : wave (l-1) (v0 U.++ v1) 
    where (v1,v0) = U.partition (test False True . testB (64-l)) v
          bv = U.map (testB (64-l)) v
{-# INLINE wave #-}

testB :: Int -> Int -> Bit
testB l = Bit . flip testBit l
{-# INLINE testB #-}

test :: a -> a -> Bit -> a
test f t b | b == Bit False = f
           | otherwise = t
{-# INLINE test #-}

offset :: P.SucBV bv => Bit -> bv -> Int
offset b bv = test 0 (P.rank (Bit False) (P.size bv) bv) b
{-# INLINE offset #-}

wmAccess :: Int -> WaveletMatrix -> Int
wmAccess i (WaveletMatrix bm) = acc 0 i
    where acc 64 _ = 0
          acc l n = g + (acc (l+1) n')
            where bs = bm B.! l
                  b = P.access n bs
                  n' = offset b bs + P.rank b n bs
                  g = if getBit b then setBit 0 l else 0 :: Int
          {-# INLINE acc #-}
{-# INLINE wmAccess #-}

wmRank :: Int -> Int -> WaveletMatrix -> Int
wmRank c i (WaveletMatrix bm) = rnk 0 0 i
    where rnk 64 n m = m - n
          rnk l n m = rnk (l+1) n' m'
            where bs = bm B.! l
                  b = testB l c
                  z = offset b bs
                  m' = z + P.rank b m bs
                  n' = z + P.rank b n bs
          {-# INLINE rnk #-}
{-# INLINE wmRank #-}

wmSelect :: Int -> Int -> WaveletMatrix -> Int
wmSelect c j (WaveletMatrix bm) = sel 0 0 j
    where sel 64 n m = n + m
          sel l n m = select b (m' - z) bs
            where bs = bm B.! l
                  b = testB l c
                  z = offset b bs
                  n' = z + P.rank b n bs
                  m' = sel (l+1) n' m
          {-# INLINE sel #-}
{-# INLINE wmSelect #-}


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
import qualified Data.Vector.Unboxed.Mutable as MU
import qualified Data.Vector.Internal.Check as Ck
import Data.STRef
import Control.Monad
import Control.Monad.ST (ST, runST)
import Prelude hiding (null)
import Data.List as Ls


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
bbsize = 2^9 `div` 64
lbsize = 2^11 `div` 64
ubsize = 2^32 `div` 64


-- |
-- >>> showTable . build $ U.fromList ((take 45 (repeat 1)) ++ (take 12 (repeat 0)))

showTable :: (U.Vector Word64,U.Vector Word64) -> [Word64]
showTable (ub,lb) = [ p | i <- U.toList lb, p <- [f 0 i, f 1 i, f 2 i, f 3 i]]
    where f 0 i = i .&. (2^32 -1)
          f n i = ((i `unsafeShiftR` ((n-1) * 10 + 32)) .&. (2^10-1)) + f (n-1) i


build :: U.Vector Word64 -> (U.Vector Word64,U.Vector Word64)
build vs = runST $ do 
  sumbb <- newSTRef (0 :: Word64)
  sumlb <- newSTRef (0 :: Word64)
  sumub <- newSTRef (0 :: Word64)
  uindv <- MU.new ublen :: ST s (MU.STVector s Word64)
  lindv <- MU.new lblen :: ST s (MU.STVector s Word64)
  let loop i = when (i < len) $ do
                 let (ui,uo,li,lo,bi,bo) = sep i
                 when (uo == 0) $ do
                   ub <- readSTRef sumub
                   MU.write uindv ui ub
                   writeSTRef sumlb 0
                 when (lo == 0) $ do
                   lb <- readSTRef sumlb
                   MU.write lindv li lb
                   modifySTRef' sumub (+ lb)
                 when (bo == 0) $ do
                   when (bi > 0 ) $ do
                     d <- MU.read lindv li
                     bb <- readSTRef sumbb
                     MU.write lindv li (d + (bb `unsafeShiftL` ((bi - 1) * 10 + 32)))
                     modifySTRef' sumlb (+ bb)
                     writeSTRef sumbb 0
                 modifySTRef' sumbb (+ pop i)
                 loop (i+1)
  loop 0
  uindv' <- U.freeze uindv
  lindv' <- U.freeze lindv
  return (uindv',lindv')
  where len = U.length vs
        ublen = len `divCeil` ubsize
        lblen = len `divCeil` lbsize
        sep i = let (ui,uo) = i `divMod` ubsize
                    (li,lo) = i `divMod` ubsize
                    (bi,bo) = lo `divMod` bbsize
                in (ui,uo,li,lo,bi,bo) :: (Int,Int,Int,Int,Int,Int)
        pop i = fromIntegral $ popCount (vs U.! i)



bbsum :: Int -> U.Vector Word64 -> U.Vector Word64
bbsum n vs = G.unfoldr chunk vs
  where chunk xs | U.null xs = Nothing
                 | otherwise = let (a,b) = (U.splitAt n xs) in Just (U.sum . U.map (fromIntegral . popCount) $ a,b)


divCeil :: Int -> Int -> Int
divCeil n b = (n + b - 1) `div` b


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

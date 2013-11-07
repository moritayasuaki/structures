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
                deriving (Eq,Ord,Show,Read)


instance SucBV Poppy where
    isobv = _Poppy
    rank (Bit True) n (Poppy len (V_Bit _ ws) ubs lbs) = c + c'
      where (iw,ow) = n `divMod` 64
            up = iw `div` ubsize
            (lp,lo) = iw `divMod` lbsize
            f 0 i = i .&. (2^32 -1)
            f n i = ((i `unsafeShiftR` ((n-1) * 10 + 32)) .&. (2^10-1)) + f (n-1) i
            c = popCount ((ws U.! iw) .&. (2 ^ ow - 1))
            c' = fromIntegral ((ubs U.! up) + f lo (lbs U.! lp))
    rank _ n p = n - rank (Bit True) n p
    select b n poppy@(Poppy len _ _ _) = search (\i -> n < rank b i poppy) 0 (len-1)

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

buildPoppy :: U.Vector Word64 -> (U.Vector Word64,U.Vector Word64)
buildPoppy vs = runST $ do 
  sumlb <- newSTRef (0 :: Word64)
  sumub <- newSTRef (0 :: Word64)
  uindv <- MU.new ublen :: ST s (MU.STVector s Word64)
  lindv <- MU.new lblen :: ST s (MU.STVector s Word64)
  let loop i = do when (i < len) $ do
                    let (ui,uo,li,lo,bi,_) = sep i
                    when (uo == 0) $ do
                      ub <- readSTRef sumub
                      MU.write uindv ui ub
                      writeSTRef sumlb 0
                    when (lo == 0) $ do
                      lb <- readSTRef sumlb
                      MU.write lindv li lb
                      modifySTRef' sumub (+ lb)
                    let r = (safeSlice i bbsize vs) :: U.Vector Word64
                        bbsum = fromIntegral . U.sum . U.map popCount $ r
                    modifySTRef' sumlb (+ bbsum)
                    when (bi /= (lbsize `div` bbsize) - 1 ) $ do
                      d <- MU.read lindv li
                      MU.write lindv li (d + (bbsum `unsafeShiftL` (bi * 10 + 32)))
                    loop (i+bbsize)
  loop 0
  uindv' <- U.freeze uindv
  lindv' <- U.freeze lindv
  return (uindv',lindv')
  where len = U.length vs
        ublen = len `divCeil` ubsize
        lblen = len `divCeil` lbsize
        sep i = let (ui,uo) = i `divMod` ubsize
                    (li,lo) = i `divMod` lbsize
                    (bi,bo) = lo `divMod` bbsize
                in (ui,uo,li,lo,bi,bo) :: (Int,Int,Int,Int,Int,Int)
        safeSlice i s vs = U.take s (U.drop i vs)


divCeil :: Int -> Int -> Int
divCeil n b = (n + b - 1) `div` b

 


-- |
-- >>> search (\n->n^2 >= 16) 0 5
-- 4
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

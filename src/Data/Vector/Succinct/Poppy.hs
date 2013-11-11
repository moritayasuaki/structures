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

module Data.Vector.Succinct.Poppy (
  Poppy, CSPoppy, _Poppy, _CSPoppy
  ) where

import Data.Bits
import Data.Vector.Array
import Control.Lens as L
import Data.Vector.Bit hiding (rank)
import Data.Word
import qualified Data.Vector.Generic as G
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as MU
import qualified Data.Vector.Internal.Check as Ck
import Data.STRef
import Control.Monad
import Control.Monad.ST (ST, runST)
import Prelude hiding (null)
import Data.List as Ls
import Data.Maybe


#define BOUNDS_CHECK(f) (Ck.f __FILE__ __LINE__ Ck.Bounds)

class SucBV sbv where
    isobv :: Iso' sbv (Array Bit)
    rank :: Bit -> Int -> sbv -> Int
    select :: Bit -> Int -> sbv -> Int
    size :: sbv -> Int


-- |
-- >>> let t = Bit True
-- >>> let f = Bit False
-- >>> let p@(Poppy i a ub lb)  = _Poppy # U.fromList (take 30000 (cycle [t,f,f,f,f]))
-- >>> rank t 8886 p
-- 1778
-- >>> select t 1778 p
-- 8886
-- >>> let csp = _CSPoppy # U.fromList (take 50000 (cycle [t,f,f,f,f]))
-- >>> rank t 8888 csp
-- 1778
-- >>> select t 1778 csp
-- 8886

data Poppy = Poppy {-# UNPACK #-} !Int !(Array Bit) 
                !(U.Vector Word64) 
                !(U.Vector Word64)
                deriving (Eq,Ord,Show,Read)

instance SucBV Poppy where
    isobv = _Poppy
    rank (Bit True) n (Poppy len (V_Bit _ ws) ubs lbs) = c''' + c'' + c' + c
      where (wi,wo) = n `divMod` 64
            (ui,uo) = wi `divMod` ubsize
            (li,lo) = wi `divMod` lbsize
            (bi,bo) = wi `divMod` bbsize
            f 0 i = i .&. (2^32 - 1)
            f n i = ((i `unsafeShiftR` ((n-1) * 10 + 32)) .&. (2^10-1)) + f (n-1) i
            c = popCount ((ws U.! wi) .&. (2 ^ wo - 1))
            c' = U.foldl' (\p w-> p + popCount w) 0 (U.unsafeSlice (bi*bbsize) bo ws)
            c'' = fromIntegral (f (lo `div` bbsize) (lbs U.! li))
            c''' = fromIntegral (ubs U.! ui)
    rank _ n p = n - rank (Bit True) n p
    select b n poppy@(Poppy len _ _ _) = bsearch (\i -> (n-1) < rank b i poppy) 0 (len-1)
    size (Poppy len _ _ _) = len


_Poppy :: Iso' Poppy (Array Bit)
_Poppy = iso (\(Poppy _ v _ _) -> v) f
  where f v@(V_Bit n ws) = let (ubs,lbs) = buildPoppy ws in Poppy n v ubs lbs

-- Poppy
--
-- blocks                          indices
-- basic block size = 2^9 bits     3/4*10 bits par 2^9 bits
-- lower block size = 2^11 bits    32 bits par 2^11 bits
-- upper block size = 2^32 bits    64 bits par 2^32 bits
--                                 3.12500149% additional space
bbsize = 2^9 `div` 64  -- 8 words
lbsize = 2^11 `div` 64 -- 4 base blocks
ubsize = 2^32 `div` 64 -- 2000000 lower blocks

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
-- >>> bsearch (\n->n^2 >= 16) 0 5
-- 4
bsearch :: Integral n => (n -> Bool) -> n -> n -> n
bsearch p l h | l == h = l
             | p m = bsearch p l m
             | otherwise = bsearch p (m+1) h
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
                  deriving (Eq,Ord,Show,Read)

_CSPoppy :: Iso' CSPoppy (Array Bit)
_CSPoppy = iso (\(CSPoppy _ v _ _ _ _) -> v) f
  where f v = let p@(Poppy n v' ubs lbs) = _Poppy # v
                  tstbl = buildCS (Bit True) p
                  fstbl = buildCS (Bit False) p
              in CSPoppy n v' ubs lbs tstbl fstbl

buildCS :: Bit -> Poppy -> U.Vector Word32
buildCS b p@(Poppy n v' ubs lbs) = U.fromList $ map (fromIntegral . flip (select b) p) [1, 1+samplingWidth .. smax]
    where smax = rank b n p
          samplingWidth = 8192

mkbbvector :: Word64 -> U.Vector Word64
mkbbvector w = U.fromList [l0,l1,l2,l3]
    where l0 = 0
          l1 = (w `unsafeShiftR` 32) .&. (2^10-1)
          l2 = l1 + ((w `unsafeShiftR` 42) .&. (2^10-1))
          l3 = l2 + ((w `unsafeShiftR` 52) .&. (2^10-1))

instance SucBV CSPoppy where
    isobv = _CSPoppy
    size (CSPoppy len _ _ _ _ _) = len
    rank (Bit True) n (CSPoppy len (V_Bit _ ws) ubs lbs _ _) = c''' + c'' + c' + c
      where (wi,wo) = n `divMod` 64
            (ui,uo) = wi `divMod` ubsize
            (li,lo) = wi `divMod` lbsize
            (bi,bo) = wi `divMod` bbsize
            f 0 i = i .&. (2^32 - 1)
            f n i = ((i `unsafeShiftR` ((n-1) * 10 + 32)) .&. (2^10-1)) + f (n-1) i
            c = popCount ((ws U.! wi) .&. (2 ^ wo - 1))
            c' = U.foldl' (\p w-> p + popCount w) 0 (U.unsafeSlice (bi*bbsize) bo ws)
            c'' = fromIntegral (f (lo `div` bbsize) (lbs U.! li))
            c''' = fromIntegral (ubs U.! ui)
    rank _ n p = n - rank (Bit True) n p
    select (Bit True) n (CSPoppy len (V_Bit _ ws) ubs lbs tstbl _) = eli * 2^11 + bi * 2^9 + wi * 2^6 + biti
      where ui = maybe (U.length ubs - 1) pred (U.findIndex (\a -> (fromIntegral n) < a) ubs)
            ur = ubs U.! ui
            t = fromIntegral n - ur
            tbli = ((n-1) `div` 8192)
            sample = fromIntegral $ tstbl U.! tbli 
            nli =  fromIntegral $ (sample `div` 64) `div` lbsize
            lbs' = U.unsafeDrop nli lbs
            eli = nli + maybe (U.length lbs' - 1) pred (U.findIndex (\a -> t < a .&. (2^32-1)) lbs')
            lr = lbs U.! eli
            t' = t - (lr .&. (2^32-1))
            bs = mkbbvector lr
            bi = maybe (U.length bs - 1) pred (U.findIndex (\a -> t' < a) bs)
            br = bs U.! bi
            ws' = U.scanl (\l a -> l + fromIntegral (popCount a)) 0 $ U.drop (eli * 2^5 + bi * 2^3) ws
            t'' = t' - br
            wi = maybe (U.length ws' - 1) pred (U.findIndex (\a -> t'' < a) ws')
            wr = ws' U.! wi
            t''' = fromIntegral (t'' - wr)
            biti = select64naive t''' (ws U.! (eli * 2^5 + bi * 2^3 + wi))

select64naive :: Int -> Word64 -> Int
select64naive r x = maybe 64 id $ findIndex (r <=) [popCount (x .&. (2^i-1)) | i <- [0..63]]


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
  , SucBV(..)
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
    access :: Int -> sbv -> Bit
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
    access i (Poppy _ v _ _) = v U.! i
    rank b@(Bit True) n (Poppy len (V_Bit _ ws) ubs lbs) = 
        ubRank b ui ubs + lbRank b li lbs + bbRank b (lo `div` bbsize) (lbs U.! li) + wbRank b bo (U.unsafeDrop (bi*bbsize) ws) + wRank b wo (ws U.! wi)
      where (wi,wo) = n `divMod` 64
            (ui,uo) = wi `divMod` ubsize
            (li,lo) = wi `divMod` lbsize
            (bi,bo) = wi `divMod` bbsize
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
    access i (CSPoppy _ v _ _ _ _) = v U.! i
    size (CSPoppy len _ _ _ _ _) = len
    rank b@(Bit True) n (CSPoppy len (V_Bit _ ws) ubs lbs _ _) = 
        ubRank b ui ubs + lbRank b li lbs + bbRank b (lo `div` bbsize) (lbs U.! li) + wbRank b bo (U.unsafeDrop (bi*bbsize) ws) + wRank b wo (ws U.! wi)
      where (wi,wo) = n `divMod` 64
            (ui,uo) = wi `divMod` ubsize
            (li,lo) = wi `divMod` lbsize
            (bi,bo) = wi `divMod` bbsize
    rank _ n p = n - rank (Bit True) n p
    select b j (CSPoppy len (V_Bit _ ws) ubs lbs tstbl fstbl) = li * 2^11 + bi * 2^9 + wi * 2^6 + biti
      where tbl = if getBit b then tstbl else fstbl
            ui = pred $ ubSelect b j ubs
            j' = j - ubRank b ui ubs
            tbli = ((j-1) `div` 8192)
            sample = fromIntegral $ tbl U.! tbli 
            nli = fromIntegral $ (sample `div` 64) `div` lbsize
            lbs' = U.unsafeDrop nli lbs
            li = pred $ nli + lbSelect b j' lbs' 
            j'' = j' - lbRank b li lbs
            bi = pred $ bbSelect b j'' (lbs U.! li)
            j''' = j'' - bbRank b bi (lbs U.! li)
            ws' = U.unsafeDrop (li * 2^5 + bi * 2^3) ws
            wi = pred $ wbSelect b j''' ws'
            j'''' = j''' - wbRank b wi ws'
            biti = wSelect b j'''' (ws' U.! wi)

lsearch :: Int -> Int -> (Int -> Bool) -> Int
lsearch s e f | s == e = e
              | f s = s
              | otherwise = lsearch (s+1) e f

ubRank :: Bit -> Int -> U.Vector Word64 -> Int
ubRank (Bit True) i ubs = fromIntegral $ ubs U.! i
ubRank (Bit False) i ubs = i*2^32 - ubRank (Bit True) i ubs

lbRank :: Bit -> Int -> U.Vector Word64 -> Int
lbRank (Bit True) i lbs = fromIntegral $ (lbs U.! i) .&. (2^32-1)
lbRank (Bit False) i lbs = i*2^11 - lbRank (Bit True) i lbs

bbRank :: Bit -> Int -> Word64 -> Int
bbRank (Bit True) 0 bbs = 0
bbRank (Bit True) i bbs = fromIntegral ((bbs `unsafeShiftR` ((i-1) * 10 + 32)) .&. (2^10 - 1)) + bbRank (Bit True) (i-1) bbs 
bbRank (Bit False) i bbs = i * 2^9 - bbRank (Bit True) i bbs 

wbRank :: Bit -> Int -> U.Vector Word64 -> Int
wbRank (Bit True) i wbs = fromIntegral $ (U.scanl (\acc w -> acc + popCount w) 0 wbs) U.! i
wbRank (Bit False) i wbs = i * 64 - wbRank (Bit True) i wbs

wRank :: Bit -> Int -> Word64 -> Int
wRank (Bit True) i w = popCount (w .&. (2^i - 1))
wRank (Bit False) i w = i - wRank (Bit True) i w

ubSelect :: Bit -> Int -> U.Vector Word64 -> Int
ubSelect b j ubs = lsearch 0 (U.length ubs) (\i -> j < ubRank b i ubs)

lbSelect :: Bit -> Int -> U.Vector Word64 -> Int
lbSelect b j lbs = lsearch 0 (U.length lbs) (\i -> j < lbRank b i lbs)

bbSelect :: Bit -> Int -> Word64 -> Int
bbSelect b j bbs = lsearch 0 (lbsize `div` bbsize) (\i -> j < bbRank b i bbs)

wbSelect :: Bit -> Int -> U.Vector Word64 -> Int
wbSelect b j wbs = lsearch 0 bbsize (\i -> j < wbRank b i  wbs)

-- |
-- >>> wSelect (Bit True) 64 0xFFFFFFFFFFFFFFFF
-- 64
-- >>> wSelect (Bit True) 8 0x0101010101010101
-- 57
wSelect :: Bit -> Int -> Word64 -> Int
wSelect (Bit True) 0 w = 0
wSelect (Bit True) j w = 
  let x1 = ((w .&. 0xaaaaaaaaaaaaaaaa) `unsafeShiftR` 1) + (w .&. 0x5555555555555555)
      x2 = ((x1 .&. 0xcccccccccccccccc) `unsafeShiftR` 2) + (x1 .&. 0x3333333333333333)
      x3 = ((x2 .&. 0xf0f0f0f0f0f0f0f0) `unsafeShiftR` 4) + (x2 .&. 0x0f0f0f0f0f0f0f0f)
      x4 = ((x3 .&. 0xff00ff00ff00ff00) `unsafeShiftR` 8) + (x3 .&. 0x00ff00ff00ff00ff)
      x5 = ((x4 .&. 0xffff0000ffff0000) `unsafeShiftR` 16) + (x4 .&. 0x0000ffff0000ffff)
      v5 = fromIntegral $ x5 .&. 0xffffffff
      (j5,p5) = if (j > v5) then (j - v5 , 32) else (j,0)
      v4 = fromIntegral $ (x4 `unsafeShiftR` p5) .&. 0xffff
      (j4,p4) = if (j5 > v4) then (j5 - v4 , p5 + 16) else (j5,p5)
      v3 = fromIntegral $ (x3 `unsafeShiftR` p4) .&. 0xff
      (j3,p3) = if (j4 > v3) then (j4 - v3 , p4 + 8) else (j4,p4)
      v2 = fromIntegral $ (x2 `unsafeShiftR` p3) .&. 0xf
      (j2,p2) = if (j3 > v2) then (j3 - v2 , p3 + 4) else (j3,p3)
      v1 = fromIntegral $ (x1 `unsafeShiftR` p2) .&. 0x3
      (j1,p1) = if (j2 > v1) then (j2 - v1 , p2 + 2) else (j2,p2)
      v0 = fromIntegral $ (w `unsafeShiftR` p1) .&. 0x01
      (j0,p0) = if (j1 > v0) then (j1 - v0 , p1 + 1) else (j1,p1)
  in p0 + 1
wSelect (Bit False) j w = wSelect (Bit True) j (complement w)


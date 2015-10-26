{-# LANGUAGE BangPatterns, MultiWayIf #-}

module Data.Smashy.Shared where

import qualified Data.Vector.Storable.ByteString as BS
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Char8           as C8
import qualified Data.ByteString.Internal        as C8
import qualified Data.Vector.Storable            as VS
import Data.Vector.Storable ((!))
import qualified Data.Vector.Storable.MMap       as MM
import qualified Data.Vector.Storable.Mutable    as VM
import qualified Foreign.ForeignPtr              as FP
import Data.Word
import Data.Bits ((.|.), shiftL, shiftR)
import Control.Monad.Primitive  (PrimMonad, PrimState)
import Data.Smashy.Constants
import Data.Smashy.Encoding
import Data.Smashy.Types

getMem :: (VM.Storable a, Num a) => DataLocation -> Int -> IO (VM.IOVector a)
getMem (OnDisk fp) size = MM.unsafeMMapMVector fp MM.ReadWriteEx (Just (0, size))
getMem       InRam size = emptyTable
    where
    emptyTable :: (PrimMonad m, Num a, VM.Storable a) => m (VM.MVector (PrimState m) a)
    emptyTable = VM.new size >>= \t -> VM.set t 0 >> return t

pad0 :: (VM.Storable a, Num a, PrimMonad m) => VM.MVector (PrimState m) a -> m (VM.MVector (PrimState m) a)
pad0 dat = VM.write dat 0 0 >> return dat

unmapData :: (VM.Storable a) => VM.IOVector a -> IO ()
unmapData = (\(ptr,_,_) -> FP.finalizeForeignPtr (FP.castForeignPtr ptr)) . VM.unsafeToForeignPtr

expandRamList :: TermList a -> IO (TermList a)
expandRamList (TermList position size termData space) = do
    let increase = size
        newSize = size + increase
    newData <- VM.grow termData increase
    return (TermList position newSize newData (space + increase))

expandDiskTermList :: FilePath -> TermList a -> IO (TermList a)
expandDiskTermList fp (TermList position size termData space) = do
    let increase = size
        newSize = size + increase
    newData <- resizeMappedFile fp termData newSize
    return (TermList position newSize newData (space + increase))

resizeMappedFile :: (VM.Storable a) => FilePath -> VM.IOVector a -> Int -> IO (VM.IOVector a)
resizeMappedFile fp mmapData newSize = do
    unmapData mmapData
    MM.unsafeMMapMVector fp MM.ReadWriteEx (Just (0, newSize))

--avoid this snoc somehow?
termAt :: PrimMonad m => VM.MVector (PrimState m) Word8 -> Int -> m Escaped
termAt dat = termAt' BS.empty False
    where
    termAt' buf esc p = do
        x <- VM.read dat p
        if | esc              -> termAt' (BS.snoc buf x) False (p+1)
           | x == sepCharW    -> return $ Escaped (BS.snoc buf x)
           | x == escapeCharW -> termAt' (BS.snoc buf x)  True (p+1)
           | otherwise        -> termAt' (BS.snoc buf x) False (p+1)

isHashFull :: HashTable a -> Bool
isHashFull (HashTable sz _ sp) = fromIntegral sp < (1.0 - maxFillFactor) * fromIntegral sz

writeTerm :: PrimMonad m => VM.MVector (PrimState m) Word8 -> Position -> Escaped -> m Position
writeTerm v p (Escaped k) = writeBytes v p k

writeBytes :: PrimMonad m => VM.MVector (PrimState m) Word8 -> Position -> C8.ByteString -> m Position
writeBytes termData pos t = do
    let len = C8.length t
        target = VM.unsafeSlice pos len termData  
    source <- VS.unsafeThaw (BS.byteStringToVector t)
    VM.unsafeCopy target source
    return (pos + len)

compareTerm :: PrimMonad m => VM.MVector (PrimState m) Word8 -> Position -> Escaped -> m TermComparison
compareTerm termData lpos (Escaped k) =
  go True 0 (BS.length k) False lpos (VM.length termData) False
  where
  go !m i x ea j y eb
      | i == x || j == y = return DS --Ran off end
      | otherwise = do      
          let a = BS.index k i
          b <- VM.read termData j          
          if a == sepCharW && not ea && b == sepCharW && not ea
              then return (if m then Matched else SSDV)
              else go (m && a == b) (i+1) x (a == escapeCharW && not ea) (j+1) y (b == escapeCharW && not eb)

{-# INLINE assemble #-}
assemble :: Word32 -> Word32 -> Word64
assemble upper lower = shiftL (fromIntegral upper :: Word64) 32 .|. (fromIntegral lower :: Word64)

--Takes one word64 and splits it into its two 32-bit components
{-# INLINE disassemble #-}
disassemble :: Word64 -> (Word32, Word32)
disassemble combined =
    let upper = fromIntegral (shiftR combined 32) :: Word32
        lower = fromIntegral combined :: Word32 in
        (upper, lower)

{-# INLINE w8sTow32 #-}
w8sTow32 :: VS.Vector Word8 -> Word32
w8sTow32 v = do
    let v' = VS.map (\x -> fromIntegral x :: Word32) v
    shiftL (v' ! 0) 24
        .|. shiftL (v' ! 1) 16
        .|. shiftL (v' ! 2) 8 
        .|. (v' ! 3)
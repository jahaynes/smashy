{-# LANGUAGE BangPatterns #-}

module Data.Smashy.Encoding where

import qualified Data.ByteString.Char8           as C8
import qualified Data.Digest.XXHash              as XX
import qualified Data.Vector.Storable            as VS
import qualified Data.Vector.Storable.ByteString as BS
import qualified Data.Vector.Storable.Mutable    as VM

import Data.Word (Word8, Word32)
import Data.ByteString.Internal (c2w)

sepChar :: Char
sepChar = '\n'

sepCharW :: Word8
sepCharW = 10

escapeChar :: Char
escapeChar = '\\'

escapeCharW :: Word8
escapeCharW = 92

newtype Escaped = Escaped C8.ByteString deriving Show

{-# INLINE isSpecial #-}
isSpecial :: Char -> Bool
isSpecial c = c == sepChar || c == escapeChar

{-# INLINE fastEscape #-}
fastEscape :: C8.ByteString -> Escaped
fastEscape x =
    Escaped .
        BS.vectorToByteString $
            VS.create $ do
                v <- VM.unsafeNew (countTot x + 1)
                go v (C8.length x) 0 0

                where
                countTot :: C8.ByteString -> Int
                countTot = fst . C8.mapAccumR (\a c -> if isSpecial c then (a+2, c) else (a+1,c)) 0

                go v len i j
                    | j == len = do
                        VM.write v i (c2w sepChar)
                        return v
                    | otherwise = do
                        let c = C8.index x j
                        if isSpecial c
                            then do
                                VM.write v     i (c2w escapeChar)
                                VM.write v (i+1) (c2w c)
                                go v len (i+2) (j+1)
                            else do
                                VM.write v i (c2w c)
                                go v len (i+1) (j+1)

{-# INLINE fastUnescape #-}
fastUnescape :: Escaped -> C8.ByteString
fastUnescape (Escaped bs) = do
    let len = C8.length bs
        escCount = countEscapes 0 len 0 
        newStrLen = len - escCount
    BS.vectorToByteString $ VS.create $ VM.unsafeNew newStrLen >>= \v -> go False v newStrLen 0 0
    where
    go esc v newStrLen i j
        | j == newStrLen = return v
        | otherwise = do
            let c = C8.index bs i

            if esc
                then do
                    VM.write v j (c2w c)
                    go False v newStrLen (i+1) (j+1)
                else
                    if c == escapeChar
                        then go True v newStrLen (i+1) j
                        else do
                            VM.write v j (c2w c)
                            go False v newStrLen (i+1) (j+1)

    countEscapes :: Int -> Int -> Int -> Int
    countEscapes !acc len i
        | i >= len                    = acc
        | C8.index bs i == escapeChar = countEscapes (acc+1) len (i+2)
        | otherwise                   = countEscapes     acc len (i+1) 

escLen :: Escaped -> Int
escLen (Escaped bs) = C8.length bs

asVector :: Escaped -> VS.Vector Word8
asVector (Escaped bs) = BS.byteStringToVector bs

hash :: Escaped -> XX.XXHash
hash (Escaped bs) = XX.xxHash' . C8.init $ bs
{-# LANGUAGE BangPatterns, MultiWayIf #-}

module Data.Smashy.Set where

import qualified Data.ByteVector                 as BV
import qualified Data.Vector.Storable            as VS
import qualified Data.Vector.Storable.Mutable    as VM
import Control.Monad                (foldM)
import Data.Word

import qualified System.Directory                as SD
import qualified Data.Serialize                  as CE
import qualified Data.ByteString.Char8           as C8
import Control.Monad.Primitive  (PrimMonad, PrimState)

import Data.Smashy.Types
import Data.Smashy.Constants
import Data.Smashy.Encoding
import Data.Smashy.Shared
import Data.Smashy.Util

import Prelude hiding (elem)

toDisk :: HashSet a -> IO (HashSet a)
toDisk d@(HashSet (Disk _) _ _) = return d
toDisk (HashSet (Hybrid fp)
          (HashTable hashSize hashData hashSpace)
          termList) = do

    let newLoc = Disk fp
    hashDest <- getMem (getHashLocation newLoc) (fromIntegral hashSize)
    VM.copy hashDest hashData
    unmapData hashData

    return (HashSet newLoc
               (HashTable hashSize hashDest hashSpace)
               termList)

close :: HashSet a -> IO ()
close (HashSet loc
          (HashTable hashSize hashData hashSpace)
          (TermList termPosition termSize termData termSpace)) = do
              unmapData hashData
              unmapData termData
              case getBasePath loc of
                  Nothing -> return ()
                  Just fp -> writeFile (fp ++ "/metaset")
                                 (show (hashSize, hashSpace,
                                     termPosition, termSize, termSpace))

open :: Location -> IO (HashSet a)
open loc = case getBasePath loc of
        Nothing -> error "Can't 'open' a RAM hashset"
        Just fp -> do
            (hashSize, hashSpace, termPosition, termSize, termSpace) <- fmap read (readFile (fp ++ "/metaset"))

            hashData <- getMem (getHashLocation loc) (fromIntegral hashSize)

            termData <- getMem (getKeysLocation loc) termSize

            return $ HashSet loc
                 (HashTable hashSize hashData hashSpace)
                 (TermList termPosition termSize termData termSpace)

new :: Location -> IO (HashSet a)
new loc = do
    clear loc

    let (hashLocation, termLocation) =
            case loc of
                Ram       -> (InRam, InRam)
                Hybrid fp -> (InRam, OnDisk (fp ++ keyFileName))
                Disk fp   -> (OnDisk (fp++hashFileName), OnDisk (fp ++ keyFileName))

    hashData <- getMem hashLocation startingHashSize

    termData <- getMem termLocation startingKeyListSize >>= pad0

    return $ HashSet loc
               (HashTable (fromIntegral startingHashSize) hashData startingHashSize)
               (TermList 1 startingKeyListSize termData (startingKeyListSize - 1))

rehash :: HashSet a -> IO (HashSet a)
rehash (HashSet loc ht@(HashTable sz dat _) tl) = do

    let newSize = getHashSize ht * 2 - 1
    case getHashLocation loc of

        OnDisk fp -> do
            let tempHashFile = fp ++ "_"
            removeIfExists tempHashFile
            xt <- getMem (OnDisk tempHashFile) (fromIntegral newSize)
            ht' <- goHash tl (HashTable newSize xt (fromIntegral newSize))
            unmapData dat
            SD.renameFile tempHashFile fp
            return $ HashSet loc ht' tl

        InRam -> do
            xt <- getMem InRam (fromIntegral newSize)
            ht' <- goHash tl (HashTable newSize xt (fromIntegral newSize))
            unmapData dat
            return $ HashSet loc ht' tl

    where
    goHash :: TermList a -> HashTable Word32 -> IO (HashTable Word32)
    goHash (TermList _ _ kdat _) (HashTable sz' dat' spc') = goHash' 0 (fromIntegral sz) spc'
        where
        goHash' :: Int -> Int -> Int -> IO (HashTable Word32)
        goHash' !p s !sp
            | p == s = return (HashTable sz' dat' sp)
            | otherwise = do
                hd <- VM.read dat p
                if hd == 0
                    then goHash' (p+1) s sp
                    else do
                        k <- termAt kdat (fromIntegral hd)
                        writeHash Nothing k hd
                        goHash' (p+1) s (sp-1)

            where
            writeHash :: Maybe Word32 -> Escaped -> Word32 -> IO ()
            writeHash mph ek hd = do

                let h = (case mph of
                            Nothing -> hash ek
                            Just ph -> ph + 1) `mod` sz'

                VM.read dat' (fromIntegral h) >>= \x ->
                    if x == 0
                        then VM.write dat' (fromIntegral h) hd
                        else writeHash (Just h) ek hd

{-# RULES "elem/bytestring" forall hs k. elem hs k = elemBs hs k #-}
{-# INLINE[1] elem #-}
elem :: (CE.Serialize a) => HashSet a -> a -> IO Bool               
elem hs k = elemBs hs . CE.encode $ k
    
elemBs :: HashSet a -> C8.ByteString -> IO Bool
elemBs hs k = do
    let ek = if C8.any isSpecial k
                 then fastEscape k
                 else Escaped $ k `C8.snoc` sepChar

    po <- probeForNextSetPosition True hs ek
    case po of
        KeyFoundAt _ -> return True
        FreeAt  _ -> return False
        x         -> error . concat $ ["The impossible happened: lkup received: ", show x]

{-# RULES "storeOne/storeOneBs" storeOne = storeOneBs #-}
{-# INLINE[1] storeOne #-}
storeOne :: (CE.Serialize a) => HashSet a -> a -> IO (HashSet a)
storeOne hs = storeOneBs hs . CE.encode 

storeOneBs :: HashSet a -> C8.ByteString -> IO (HashSet a)
storeOneBs hs_ bs =
    storeOneIntern hs_ (if C8.any isSpecial bs
                                  then fastEscape bs
                                  else Escaped $ bs `C8.snoc` sepChar)

    where
    storeOneIntern :: HashSet a -> Escaped -> IO (HashSet a)
    storeOneIntern hs@(HashSet loc ht tl) ew =
        probeForNextSetPosition False hs ew >>= \po ->
            case po of
                FreeAt h -> do
                    let termPos = getPosition tl
                    VM.write (getHashData ht) (fromIntegral h) (fromIntegral termPos)
                    termPos' <- writeTerm (getData tl) termPos ew
                    return (HashSet loc (decrTableSpace ht) (advanceTo tl termPos'))
                KeyFoundAt _ -> return hs
                TermListFull ->
                    case getKeysLocation loc of
                        InRam -> expandRamList tl >>= \tl' -> storeOneIntern (HashSet loc ht tl') ew
                        OnDisk fp -> expandDiskTermList fp tl >>= \tl' -> storeOneIntern (HashSet loc ht tl') ew
                HashTableFull -> rehash hs >>= \hs' -> storeOneIntern hs' ew

probeForNextSetPosition :: Bool -> HashSet a -> Escaped -> IO PositionInfo
probeForNextSetPosition onlyReading (HashSet _ ht tl) ew
    | onlyReading             = notfull
    | getSpace tl < escLen ew = return TermListFull
    | isHashFull ht           = return HashTableFull
    | otherwise               = notfull

    where
    notfull = probeForNextSetPosition' (-1) (hash ew `mod` getHashSize ht)

    probeForNextSetPosition' :: Word32 -> Word32 -> IO PositionInfo
    probeForNextSetPosition' firstAttempt h
        | h == firstAttempt = return HashTableFull
        | otherwise = do
            hashEntry <- VM.read (getHashData ht) (fromIntegral h) :: IO Word32
            if hashEntry == 0
                then return $ FreeAt h
                else do
                    matched <- compareTerm (getData tl) (fromIntegral hashEntry) ew
                    case matched of
                        Matched -> return (KeyFoundAt h)
                        _       -> probeForNextSetPosition'
                                       (if firstAttempt == -1 then h else firstAttempt)
                                       (if h + 1 == getHashSize ht then 0 else h + 1)

{-# RULES "terms/Bs" terms = termsBs #-}
{-# INLINE[1] terms #-}
terms :: CE.Serialize k => a -> (a -> k -> IO a) -> HashSet k -> IO a
terms acc f hs = go acc (getData . getSetTerms $ hs) (getPosition . getSetTerms $ hs) f 1
    where
    go :: CE.Serialize k => a -> VM.IOVector Word8 -> Int -> (a -> k -> IO a) -> Int -> IO a
    go !acc dat len f n = do
        mn <- getWord dat len n
        case mn of
            Nothing -> return acc
            Just (w,ni) ->
                case CE.decode . fastUnescape $ w of
                    Left l -> error l
                    Right r -> do
                        acc' <- f acc r
                        go acc' dat len f ni

termsBs :: a -> (a -> C8.ByteString -> IO a) -> HashSet C8.ByteString -> IO a
termsBs acc f hs = goBs acc (getData . getSetTerms $ hs) (getPosition . getSetTerms $ hs) f 1
    where
    goBs :: a -> VM.IOVector Word8 -> Int -> (a -> C8.ByteString -> IO a) -> Int -> IO a
    goBs !acc dat len f n = do
        mn <- getWord dat len n
        case mn of
            Nothing -> return acc
            Just (w,ni) -> do
                acc' <- f acc (fastUnescape w)
                goBs acc' dat len f ni

{-# INLINABLE getWord #-}
getWord :: VM.IOVector Word8 -> Int -> Int -> IO (Maybe (Escaped, Int))
getWord vm len a
    | a >= VM.length vm = return Nothing
    | otherwise = do
        z <- nextWord' False a
        if z == -1
            then return Nothing
            else do
                buf <- VS.freeze . VM.slice a z $ vm
                return (Just (Escaped $ BV.fromByteVector buf, a + z + 1))
        where
        nextWord' :: Bool -> Int -> IO Int
        nextWord' True z = nextWord' False (z+1)
        nextWord' False z
            | z >= len = return (-1)
            | otherwise =
                VM.read vm z >>= \v ->
                    case v of
                        10 -> return (z-a)
                        92 -> nextWord' True (z+1)
                        _  -> nextWord' False (z+1)
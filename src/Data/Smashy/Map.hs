{-# LANGUAGE BangPatterns, MultiWayIf #-}

module Data.Smashy.Map where

import qualified Data.ByteVector                 as BV
import qualified Data.Vector.Storable            as VS
import qualified Data.Vector.Storable.Mutable    as VM
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

toDisk :: HashMap a b -> IO (HashMap a b)
toDisk d@(HashMap (Disk _) _ _ _) = return d
toDisk (HashMap (Hybrid fp)
          (HashTable hashSize hashData hashSpace)
          keyList
          valList) = do

    let newLoc = Disk fp
    hashDest <- getMem (getHashLocation newLoc) (fromIntegral hashSize)
    VM.copy hashDest hashData
    unmapData hashData

    return (HashMap newLoc
               (HashTable hashSize hashDest hashSpace)
               keyList
               valList)

close :: HashMap a b -> IO ()
close (HashMap loc
          (HashTable hashSize hashData hashSpace)
          (TermList keyPosition keySize keyData keySpace)
          (TermList valPosition valSize valData valSpace)) = do

              unmapData hashData
              unmapData keyData
              unmapData valData

              case getBasePath loc of
                  Nothing -> return ()
                  Just fp -> writeFile (fp ++ "/metahash")
                                 (show (hashSize, hashSpace,
                                     keyPosition, keySize, keySpace,
                                     valPosition, valSize, valSpace))

open :: Location -> IO (HashMap a b)
open loc = case getBasePath loc of
        Nothing -> error "Can't 'open' a RAM hashset"
        Just fp -> do
            (hashSize, hashSpace,
             keyPosition, keySize, keySpace,
             valPosition, valSize, valSpace) <- fmap read (readFile (fp ++ "/metahash"))

            hashData <- getMem (getHashLocation loc) (fromIntegral hashSize) 
            keyData <- getMem (getKeysLocation loc) keySize
            valData <- getMem (getValsLocation loc) valSize

            return $ HashMap loc
                 (HashTable hashSize hashData hashSpace)
                 (TermList keyPosition keySize keyData keySpace)
                 (TermList valPosition valSize valData valSpace)

new :: Location -> IO (HashMap a b)
new = newWithHashSize startingHashSize

newWithHashSize :: Int -> Location -> IO (HashMap a b)
newWithHashSize hashSize loc = do
    clear loc

    let (hashLocation, keyLocation, valLocation) =
            case loc of
                Ram       -> (InRam,                     InRam,                      InRam)
                Hybrid fp -> (InRam,                     OnDisk (fp ++ keyFileName), OnDisk (fp ++ valFileName))
                Disk fp   -> (OnDisk (fp++hashFileName), OnDisk (fp ++ keyFileName), OnDisk (fp ++ valFileName))

    hashData <- getMem hashLocation hashSize
    keyData <- getMem keyLocation startingKeyListSize >>= pad0
    valData <- getMem valLocation startingValListSize

    return $ HashMap loc
               (HashTable (fromIntegral hashSize) hashData hashSize)
               (TermList 1 startingKeyListSize keyData (startingKeyListSize - 1))
               (TermList 0 startingValListSize valData startingValListSize)

rehash :: HashMap a b -> IO (HashMap a b)
rehash (HashMap loc ht@(HashTable sz dat _) kl vl) = do

    let newSize = getHashSize ht * 2 - 1
    case getHashLocation loc of

        OnDisk fp -> do
            let tempHashFile = fp ++ "_"
            removeIfExists tempHashFile
            xt <- getMem (OnDisk tempHashFile) (fromIntegral newSize)
            ht' <- goHash kl (HashTable newSize xt (fromIntegral newSize))
            unmapData dat
            SD.renameFile tempHashFile fp
            return $ HashMap loc ht' kl vl

        InRam -> do
            xt <- getMem InRam (fromIntegral newSize)
            ht' <- goHash kl (HashTable newSize xt (fromIntegral newSize))
            unmapData dat
            return $ HashMap loc ht' kl vl

    where
    goHash :: TermList a -> HashTable Word64 -> IO (HashTable Word64)
    goHash (TermList _ _ kdat _) (HashTable sz' dat' spc') = goHash' 0 (fromIntegral sz) spc'
        where
        goHash' :: Int -> Int -> Int -> IO (HashTable Word64)
        goHash' !p s !sp | p == s = return (HashTable sz' dat' sp)
                        | otherwise = do
                            hd <- VM.read dat p

                            if hd == 0
                                then goHash' (p+1) s sp
                                else do
                                    k <- termAt kdat . fromIntegral . fst . disassemble $ hd
                                    writeHash Nothing k hd
                                    goHash' (p+1) s (sp-1)

            where
            writeHash :: Maybe Word32 -> Escaped -> Word64 -> IO ()
            writeHash mph ek hd = do

                let h = (case mph of
                            Nothing -> hash ek
                            Just ph -> ph + 1) `mod` sz'

                VM.read dat' (fromIntegral h) >>= \x ->
                    if x == 0
                        then VM.write dat' (fromIntegral h) hd
                        else writeHash (Just h) ek hd


{-# RULES "get/BsBs" get = getBsBs #-}
{-# RULES "get/Bs" get = getBs #-}
{-# INLINE[1] get #-}
get :: (CE.Serialize a, CE.Serialize b, Show a) => HashMap a b -> a -> IO (Maybe b)
get hm k = do
    v <- getIntern hm (CE.encode k)
    case v of
        Nothing -> return Nothing
        Just x -> case CE.decode x of
                      Left l -> error l
                      Right r -> return (Just r)

getBs :: (CE.Serialize b) => HashMap C8.ByteString b -> C8.ByteString -> IO (Maybe b)
getBs hm k = do
    v <- getIntern hm k
    case v of
        Nothing -> return Nothing
        Just x -> case CE.decode x of
                      Left l -> error l
                      Right r -> return (Just r)
                      
getBsBs :: HashMap C8.ByteString C8.ByteString -> C8.ByteString -> IO (Maybe C8.ByteString)
getBsBs = getIntern
                      
getIntern :: HashMap a b -> C8.ByteString -> IO (Maybe C8.ByteString)
getIntern hm@(HashMap _ _ _ (TermList _ _ valData _)) k = do
    
    let ek = if C8.any isSpecial k
                 then fastEscape k
                 else Escaped $ k `C8.snoc` sepChar        
                
    po <- probeForNextHashPosition True hm (ek, undefined)
    case po of
        KeyValueFoundAt vpos -> do
            r <- termAt valData (fromIntegral vpos)
            return (Just . fastUnescape $ r)
            
        FreeAt  _ -> return Nothing
        x         -> error . concat $ ["The impossible happened: getBs received: ", show x]


{-# RULES "storeOne/BsBs" storeOne = storeOneBsBs #-}
{-# RULES "storeOne/Bs" storeOne = storeOneBs #-}
{-# INLINE[1] storeOne #-}
storeOne :: (CE.Serialize a, CE.Serialize b) => HashMap a b -> (a,b) -> IO (HashMap a b)
storeOne hm (k,v) = storeOneIntern hm (CE.encode k, CE.encode v)

storeOneBs :: (CE.Serialize b) => HashMap C8.ByteString b -> (C8.ByteString, b) -> IO (HashMap C8.ByteString b)
storeOneBs hm (k,v) = storeOneIntern hm (k, CE.encode v)

storeOneBsBs :: HashMap C8.ByteString C8.ByteString -> (C8.ByteString, C8.ByteString) -> IO (HashMap C8.ByteString C8.ByteString)
storeOneBsBs hm (k,v) = storeOneIntern hm (k,v)

storeOneIntern :: HashMap a b -> (C8.ByteString, C8.ByteString) -> IO (HashMap a b)    
storeOneIntern hm_ (k,v) = do

    let [ek, ev] = map (\x ->
                           if C8.any isSpecial x
                               then fastEscape x
                               else Escaped $ x `C8.snoc` sepChar) [k,v]
                 
    storeOneIntern' hm_ (ek, ev)

    where
    storeOneIntern' :: HashMap a b -> (Escaped, Escaped) -> IO (HashMap a b)
    storeOneIntern' hm@(HashMap loc ht kl vl) (ek,ev) =
        probeForNextHashPosition False hm (ek,ev) >>= \po ->
            case po of
                FreeAt h -> do
                    let keyPos = getPosition kl
                        valPos = getPosition vl
                    VM.write (getHashData ht) (fromIntegral h) (assemble (fromIntegral keyPos) (fromIntegral valPos))
                    keyPos' <- writeTerm (getData kl) keyPos ek
                    valPos' <- writeTerm (getData vl) valPos ev 

                    return (HashMap loc (decrTableSpace ht) (advanceTo kl keyPos') (advanceTo vl valPos'))

                KeyValueFoundAt _ -> return hm

                SameSizeDifferentValue _ _ oldvalpos -> do
                    _ <- writeTerm (getData vl) (fromIntegral oldvalpos) ev
                    return hm

                DifferentSize h oldKeyPos _ -> do
                    let valPos = getPosition vl
                    VM.write (getHashData ht) (fromIntegral h) (assemble oldKeyPos (fromIntegral valPos))
                    valPos' <- writeTerm (getData vl) valPos ev
                    return (HashMap loc ht kl (advanceTo vl valPos'))

                KeyFoundAt _ -> error "impossible: KeyFoundAt in storeOne hashmap"

                HashTableFull -> do
                    --putStrLn $ "Resizing from " ++ show (getHashSize ht) ++ "... "
                    hm' <- rehash hm
                    --putStrLn $ "\tto " ++ (show . getHashSize . getMapHash $ hm')
                    storeOneIntern' hm' (ek,ev)

                TermListFull ->
                    case getKeysLocation loc of
                        InRam -> expandRamList kl >>= \kl' -> storeOneIntern' (HashMap loc ht kl' vl) (ek,ev)
                        OnDisk fp -> expandDiskTermList fp kl >>= \kl' -> storeOneIntern' (HashMap loc ht kl' vl) (ek,ev)

                ValueListFull ->
                    case getValsLocation loc of
                        InRam -> expandRamList vl >>= \vl' -> storeOneIntern' (HashMap loc ht kl vl') (ek,ev)
                        OnDisk fp -> expandDiskTermList fp vl >>= \vl' -> storeOneIntern' (HashMap loc ht kl vl') (ek,ev)

probeForNextHashPosition :: Bool -> HashMap a b -> (Escaped, Escaped) -> IO PositionInfo
probeForNextHashPosition onlyReading (HashMap _ ht kl vl) (ek,ev)
    | onlyReading = notfull
    | getSpace kl < escLen ek = return TermListFull
    | getSpace vl < escLen ev = return ValueListFull
    | isHashFull ht           = return HashTableFull
    | otherwise = notfull 

    where
    notfull = probeForNextHashPosition' (-1) (hash ek `mod` getHashSize ht)

        where
        probeForNextHashPosition' :: Word32 -> Word32 -> IO PositionInfo
        probeForNextHashPosition' firstAttempt h
            | h == firstAttempt = return HashTableFull
            | otherwise = do
                combinedHashEntry <- VM.read (getHashData ht) (fromIntegral h) :: IO Word64
                if combinedHashEntry == 0
                    then return $ FreeAt h
                    else do

                        --Disassemble the h W64 into (keyPos, valPos) W32s
                        let (keyPos, valPos) = disassemble combinedHashEntry

                        keyMatched <- compareTerm (getData kl) (fromIntegral keyPos) ek
                        case keyMatched of
                            Matched -> if onlyReading
                                           then return $ KeyValueFoundAt valPos
                                           else do
                                               cmpVal <- compareTerm (getData vl) (fromIntegral valPos) ev
                                               return $ case cmpVal of
                                                   Matched -> KeyValueFoundAt valPos
                                                   SSDV -> SameSizeDifferentValue h keyPos valPos
                                                   DS -> DifferentSize h keyPos valPos

                            _ -> probeForNextHashPosition'
                                (if firstAttempt == -1 then h else firstAttempt)
                                (if h + 1 == getHashSize ht then 0 else h + 1)

{-# RULES "terms/Bs" terms = termsBs #-}
{-# INLINE[1] terms #-}
terms :: CE.Serialize k => a -> (a -> k -> IO a) -> HashMap k v -> IO a
terms acc f hm = go acc (getData . getMapKeys $ hm) (getPosition . getMapKeys $ hm) f 1
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

termsBs :: a -> (a -> C8.ByteString -> IO a) -> HashMap C8.ByteString v -> IO a
termsBs acc f hm = goBs acc (getData . getMapKeys $ hm) (getPosition . getMapKeys $ hm) f 1
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
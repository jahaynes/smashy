module Data.Smashy.Types where

import Data.Vector.Storable.Mutable     (IOVector)
import Data.Word                        (Word8, Word32, Word64)
import Data.Smashy.Constants

data PositionInfo = KeyFoundAt Word32
                  | KeyValueFoundAt Word32
                  | SameSizeDifferentValue Word32 Word32 Word32
                  | DifferentSize Word32 Word32 Word32
                  | FreeAt Word32
                  | HashTableFull
                  | TermListFull
                  | ValueListFull deriving Show

data HashMap a b = HashMap {getMapLocation :: Location,
                            getMapHash     :: HashTable Word64,
                            getMapKeys     :: TermList a,
                            getMapValues   :: TermList b}

data HashSet a = HashSet {getSetLocation :: Location,
                          getSetHash     :: HashTable Word32,
                          getSetTerms    :: TermList a}

data HashTable a = HashTable {getHashSize     :: Word32,
                              getHashData     :: IOVector a,
                              getHashSpace    :: Int}

data TermList a = TermList {getPosition :: Position,
                            getSize     :: Int,
                            getData     :: IOVector Word8,
                            getSpace    :: Int}
                          
type Position = Int

data Location = Ram 
              | Disk FilePath
              | Hybrid FilePath deriving Show

data DataLocation = InRam
                  | OnDisk FilePath deriving (Read, Show)

decrTableSpace :: HashTable a -> HashTable a
decrTableSpace (HashTable hashSize hashData hashSpace)
              = HashTable hashSize hashData (hashSpace - 1)

advanceTo :: TermList a -> Position -> TermList a
advanceTo (TermList termPos  termSize termData termSpace) termPos'
         = TermList termPos' termSize termData (termSpace - (termPos' - termPos))

getHashLocation :: Location -> DataLocation
getHashLocation       Ram  = InRam
getHashLocation (Hybrid _) = InRam
getHashLocation  (Disk fp) = OnDisk (fp ++ hashFileName)

getKeysLocation :: Location -> DataLocation
getKeysLocation        Ram  = InRam
getKeysLocation (Hybrid fp) = OnDisk (fp ++ keyFileName)
getKeysLocation   (Disk fp) = OnDisk (fp ++ keyFileName)

getValsLocation :: Location -> DataLocation
getValsLocation        Ram  = InRam
getValsLocation (Hybrid fp) = OnDisk (fp ++ valFileName)
getValsLocation   (Disk fp) = OnDisk (fp ++ valFileName)

getBasePath :: Location -> Maybe FilePath
getBasePath loc = case loc of
                       Ram -> Nothing
                       Disk fp -> Just fp
                       Hybrid fp -> Just fp

data TermComparison = Matched
                    | SSDV
                    | DS deriving Show
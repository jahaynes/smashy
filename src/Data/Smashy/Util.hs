module Data.Smashy.Util where

import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString.Lazy.Char8     as L8
import qualified Data.Char                      as C

import Control.Exception
import System.IO.Error

import Control.Monad (unless)
import System.Directory (doesDirectoryExist, removeFile, createDirectory)

import Data.Smashy.Types
import Data.Smashy.Constants

prinCat :: [String] -> IO ()
prinCat = putStrLn . concat

filterToWords :: L8.ByteString -> [C8.ByteString]
filterToWords = map L8.toStrict . L8.words . L8.map (\x -> if C.isAlpha x then C.toLower x else ' ')

clear :: Location -> IO ()
clear        Ram  = return ()
clear   (Disk fp) = clearPossibleFilesFromPath fp
clear (Hybrid fp) = clearPossibleFilesFromPath fp

clearPossibleFilesFromPath :: FilePath -> IO ()
clearPossibleFilesFromPath fp = do
    mapM_ removeIfExists [fp ++ hashFileName, fp ++ keyFileName, fp ++ valFileName]
    doesDirectoryExist fp >>= \e -> unless e (createDirectory fp)

removeIfExists :: FilePath -> IO ()
removeIfExists fileName = removeFile fileName `catch` handleExists
    where
    handleExists e | isDoesNotExistError e = return ()
                   | otherwise = throwIO e
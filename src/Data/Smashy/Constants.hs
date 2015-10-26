module Data.Smashy.Constants where

startingHashSize :: Int
startingHashSize = 8192

startingKeyListSize :: Int
startingKeyListSize = 8192

startingValListSize :: Int
startingValListSize = 8192

maxFillFactor :: Float
maxFillFactor = 0.80

keyFileName :: String
keyFileName = "/termlist"

valFileName :: String
valFileName = "/datalist"

hashFileName :: String
hashFileName = "/hashfile"

metaFileName :: String
metaFileName = "/metafile"
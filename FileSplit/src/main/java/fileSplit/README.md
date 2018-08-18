Usage: 

FileChunk chunk = new FileChunkFactory(file).createChunksWithDefaults();
FileSplit fileSplit = new FileSplit(chunk, destination);
fileSplit.split();

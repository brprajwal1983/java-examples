package fileSplit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

class Util {

    public static Executor getExecutor(int size) {
        return Executors.newFixedThreadPool(Math.min(size, findOptimalThreads()),(runnable) -> {
            Thread t = new Thread(runnable);
            t.setDaemon(true);
            return t;
        });
    }

    private static int findOptimalThreads() {
        return Runtime.getRuntime().availableProcessors() * (101);
    }
}

public class FileSplit {

    private int skipNBytes;
    private int readNBytes;
    private int chunkPart;
    private File file;
    private String destination;
    private RandomAccessFile raf;

    public FileChunk getFileChunk() {
        return fileChunk;
    }

    private FileChunk fileChunk;

    private FileSplit(File file, int skipNBytes, int readNBytes, int chunkPart, String destination) throws FileNotFoundException {
        this.file = file;
        this.skipNBytes = skipNBytes;
        this.readNBytes = readNBytes;
        this.chunkPart = chunkPart;
        this.raf = new RandomAccessFile(file, "r");
        this.destination = destination;
    }

    FileSplit(FileChunk fileChunk, String destination) {
        this.fileChunk = fileChunk;
        this.destination = destination;
    }

    private int getCountOfBytesWritten() {
        try {
            raf.skipBytes(skipNBytes);
            byte[] bytes = new byte[readNBytes];
            raf.read(bytes, 0, readNBytes);
            raf.close();
            RandomAccessFile rf = new RandomAccessFile(destination + "/"  + file.getName() + "_" + chunkPart, "rw");
            rf.write(bytes);
            rf.close();
            return readNBytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int split() throws FileNotFoundException {
        int skipByBytes = 0;
        List<FileSplit> fileSplits = new ArrayList<>();
        for(int i = 0 ; i < fileChunk.getChunkSize() ; i++) {
            if(i == fileChunk.getChunkSize() - 1) {
                FileSplit fileSplit1 = new FileSplit(fileChunk.getFile(), skipByBytes, fileChunk.getRemainingSizeInBytes(), i, destination);
                fileSplits.add(fileSplit1);
            }
            else {
                FileSplit fileSplit = new FileSplit(fileChunk.getFile(), skipByBytes, (int) fileChunk.getProps().get(ChunkConfiguration.CHUNK_SIZE), i, destination);
                fileSplits.add(fileSplit);
                skipByBytes = skipByBytes + (int) fileChunk.getProps().get(ChunkConfiguration.CHUNK_SIZE) ;
            }
        }

        Executor optimalThreads = Util.getExecutor(fileSplits.size());
        List<CompletableFuture<Integer>> completableFutures = fileSplits.stream()
                .map(it -> CompletableFuture.supplyAsync(it::getCountOfBytesWritten, optimalThreads))
                .collect(Collectors.toList());

        return completableFutures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()).stream().reduce((x, y) -> x + y).orElseThrow(RuntimeException::new);
    }
}

interface ChunkConfiguration {

    String CHUNK_SIZE = "chunkSizeInBytes";

    default Properties getChunkConfigs() {
        Properties props = new Properties();
        props.put(CHUNK_SIZE, 1048576);
        return props;
    }

    class DefaultChunkConfiguration implements ChunkConfiguration {
    }

    ChunkConfiguration DEFAULT_CHUNK_CONFIGURATION = new DefaultChunkConfiguration();
}

interface ChunkFactory {

    FileChunk createChunks(ChunkConfiguration chunkConfiguration);

    default FileChunk createChunksWithDefaults() {
        return createChunks(ChunkConfiguration.DEFAULT_CHUNK_CONFIGURATION);
    }
}

class FileChunkFactory implements ChunkFactory {

    private File file;

    FileChunkFactory(File file) {
        this.file = file;
    }

    @Override
    public FileChunk createChunks(ChunkConfiguration chunkConfiguration) {
        return new FileChunk(chunkConfiguration.getChunkConfigs(), file);
    }
}

class FileChunk {

    private Properties props;
    private File file;
    private int chunkSize;
    private int remainingSizeInBytes;

    FileChunk(Properties chunkConfigs, File file) {
        this.props = chunkConfigs;
        this.file = file;
        this.remainingSizeInBytes = Math.toIntExact(file.length() % (int) props.get(ChunkConfiguration.CHUNK_SIZE));
        this.chunkSize = (Math.toIntExact(file.length() / (int) props.get(ChunkConfiguration.CHUNK_SIZE)) + (remainingSizeInBytes > 0 ? 1 : 0));
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getRemainingSizeInBytes() {
        return remainingSizeInBytes;
    }

    public void setRemainingSizeInBytes(int remainingSizeInBytes) {
        this.remainingSizeInBytes = remainingSizeInBytes;
    }
}
package org.apache.lucene.analysis.synonym;

import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class TestOffHeapSynonymGraphFilter extends BaseTokenStreamFactoryTestCase {

    public void testLargeOnHeapSynonymGraph() throws Exception {
        List<Double> loadTimes = new ArrayList<>();
        List<Double> processingTimes = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            TokenFilterFactory tokenFilterFactory = timed(() -> tokenFilterFactory("synonymGraph", "synonyms", "big-synonyms.txt"),
                    "Creating on-heap filter", loadTimes);
            timed(() -> processTokens(tokenFilterFactory), "Processing tokens on-heap", processingTimes);
        }
        System.out.println("All load times:");
        for (Double loadTime : loadTimes) {
            System.out.println(loadTime);
        }
        System.out.println("All processing times:");
        for (Double processingTime : processingTimes) {
            System.out.println(processingTime);
        }
    }

    public void testLargeOffHeapSynonymGraph() throws Exception {
        List<Double> loadTimes = new ArrayList<>();
        List<Double> processingTimes = new ArrayList<>();
        List<Double> reloadTimes = new ArrayList<>();
        List<Double> reloadProcessingTimes = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Path tempFile = createTempDir();
            TokenFilterFactory tokenFilterFactory = timed(() -> tokenFilterFactory("synonymGraph", "synonyms", "big-synonyms.txt",
                            "compiledSynonymsPath", tempFile.toString()),
                    "Creating off-heap filter", loadTimes);
            timed(() -> processTokens(tokenFilterFactory), "Processing tokens off-heap", processingTimes);
            TokenFilterFactory reloaded = timed(() -> tokenFilterFactory("synonymGraph", "synonyms", "big-synonyms.txt",
                            "compiledSynonymsPath", tempFile.toString()),
                    "Reloading off-heap filter", reloadTimes);
            timed(() -> processTokens(reloaded), "Processing tokens with reloaded off-heap filter", reloadProcessingTimes);
            try (Stream<Path> indexFileStream = Files.list(tempFile)) {
                for (Path indexFile : indexFileStream.toList()) {
                    Files.delete(indexFile);
                }
            }
            Files.delete(tempFile);
        }
        System.out.println("All load times:");
        for (Double loadTime : loadTimes) {
            System.out.println(loadTime);
        }
        System.out.println("All processing times:");
        for (Double processingTime : processingTimes) {
            System.out.println(processingTime);
        }
        System.out.println("All reload times:");
        for (Double reloadTime : reloadTimes) {
            System.out.println(reloadTime);
        }
        System.out.println("All reload processing times:");
        for (Double reloadProcessingTime : reloadProcessingTimes) {
            System.out.println(reloadProcessingTime);
        }
    }

    private static Void processTokens(TokenFilterFactory tokenFilterFactory) throws IOException {
        Reader reader = new InputStreamReader(TestOffHeapSynonymGraphFilter.class.getResourceAsStream("large-text.txt"));
        TokenStream stream = whitespaceMockTokenizer(reader);
        TokenStream synonymStream = tokenFilterFactory.create(stream);
        synonymStream.reset();
        while (synonymStream.incrementToken()) {
            // Do nothing
        }
        synonymStream.end();
        return null;
    }

    private interface ExceptionalSupplier<T, E extends Exception> {
        T get() throws E;
    }

    private static <T, E extends Exception> T timed(ExceptionalSupplier<T, E> supplier, String description, List<Double> times) throws E {
        long start = System.nanoTime();
        T result = supplier.get();
        double took = (System.nanoTime() - start)/1_000_000.0;
        times.add(took);
        System.out.println(description + " took " + took + " ms");
        return result;
    }
}

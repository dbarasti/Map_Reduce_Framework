package MyMapReduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.summingInt;

public class WordCount extends MapReduce<String, List<String>, String, Integer> {

    private String pathToTxtDir;

    WordCount(String directory) {
        this.pathToTxtDir = directory;
    }

    public static void main(String[] args) throws FileNotFoundException {
        String pathToTxtDir = getDirectory();
        new WordCount(pathToTxtDir).run();
    }

    private static String getDirectory() {
        System.out.println("Please, define the path for the directory that stores the txt files.\n" +
                "(Press enter for default lookup of Books dir in root project directory)");
        Scanner sc = new Scanner(System.in);
        String pathToTxtDir = sc.nextLine();
        if (pathToTxtDir.equals("")) {
            Path currentDir = Paths.get("");
            pathToTxtDir = currentDir.toAbsolutePath().toString() + "/Books/";
        }

        return pathToTxtDir;
    }

    @Override
    Stream<Pair<String, List<String>>> read() {
        Path path = Paths.get(pathToTxtDir);
        Reader reader = new Reader(path);
        Stream<Pair<String, List<String>>> readStream = null;
        try {
            readStream = reader.read();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return readStream;
    }

    @Override
    Stream<Pair<String, Integer>> map(Pair<String, List<String>> input) {
        Map<String, Integer> summedWords = input.getValue().stream()
                .flatMap(line -> Stream.of(line.split(" |,|\n|!|$|\\?|\"|\\.|“|'|;|:|\\(|\\)|-|”|’|‘|—")))
                .filter(w -> w.length() > 3)
                .map(String::toLowerCase)
                .collect(Collectors.groupingBy(Function.identity(), summingInt((e) -> 1)));
        return summedWords.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue()));
    }

    @Override
    int compare(String k1, String k2) {
        return k1.compareTo(k2);
    }

    @Override
    Integer reduce(Integer p1, Integer p2) {
        return p1 + p2;
    }

    @Override
    void write(Stream<Pair<String, Integer>> dataToWrite) {
        File outputFile = new File("./word-count.txt");
        try {
            Writer.write(outputFile, dataToWrite);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

package MyMapReduce;

import com.sun.org.apache.xpath.internal.res.XPATHErrorResources_it;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvertedIndex extends MapReduce<String, List<String>, String, Pair<String, Integer>> {
    private String pathToTxtDir;

    InvertedIndex(String directory) {
        this.pathToTxtDir = directory;
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
    Stream<Pair<String, Pair<String, Integer>>> map(Pair<String, List<String>> input) {
        List<Pair<String, Pair<String, Integer>>> mappedInput = new ArrayList<>();
        int lineNumber = 1;
        for (String s : input.getValue()) {
            List<String> words = Arrays.asList(s.split(" |,|\n|!|$|\\?|\"|\\.|“|'|;|:|\\(|\\)|-|”|’|‘|—"));
            words = words.stream().filter(w -> w.length() > 3).collect(Collectors.toList());
            for (String word : words) {
                mappedInput.add(new Pair<>(word, new Pair<>(input.getKey(), lineNumber)));
            }
            lineNumber++;
        }
        return mappedInput.stream();
    }

    @Override
    int compare(String k1, String k2) {
        return k1.compareTo(k2);
    }

    @Override
    Pair<String, Integer> reduce(Pair<String, Integer> p1, Pair<String, Integer> p2) {
        return null;
    }

    @Override
    void write(Stream<Pair<String, Pair<String, Integer>>> dataToWrite) {
        dataToWrite.forEach(System.out::println);
    }

    public static void main(String [] args) {
        String pathToTxtDir = getDirectory();
        new InvertedIndex(pathToTxtDir).run();
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
}

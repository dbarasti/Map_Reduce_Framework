package MyMapReduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InvertedIndex extends MapReduce<String, List<String>, String, List<Pair<String, Integer>>> {
    private String pathToTxtDir;

    InvertedIndex(String directory) {
        this.pathToTxtDir = directory;
    }

    public static void main(String[] args) {
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

    /**
     * @return returns a stream of <word,<filename, linenumber>>
     */
    @Override
    Stream<Pair<String, List<Pair<String, Integer>>>> map(Pair<String, List<String>> input) {
        List<Pair<String, List<Pair<String, Integer>>>> mappedInput = new ArrayList<>();
        int lineNumber = 1;
        for (String line : input.getValue()) {
            List<String> wordsInLine = Arrays.asList(line.split(" |,|\n|!|$|\\?|\"|\\.|“|'|;|:|\\(|\\)|-|”|’|‘|—|_"));
            wordsInLine = wordsInLine.stream()
                    .filter(w -> w.length() > 3)
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
            for (String word : wordsInLine) {
                mappedInput.add(new Pair<>(word, Collections.singletonList(new Pair<>(input.getKey(), lineNumber))));
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
    List<Pair<String, Integer>> reduce(List<Pair<String, Integer>> p1, List<Pair<String, Integer>> p2) {
        List<Pair<String, Integer>> reduction = new ArrayList<>(p1);
        reduction.addAll(p2);
        return reduction;
    }

    @Override
    void write(Stream<Pair<String, List<Pair<String, Integer>>>> dataToWrite) {
        File outputFile = new File("./inverted-index.txt");
        PrintStream ps = null;
        try {
            ps = new PrintStream(outputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        PrintStream finalPs = ps;
        dataToWrite.sorted(Comparator.comparing(Pair::getKey))
                .forEach(p -> p.value.forEach(info -> {
                    assert finalPs != null;
                    finalPs.println(p.getKey() + ", " + info.getKey() + ", " + info.getValue());
                }));
        assert ps != null;
        ps.close();
    }
}

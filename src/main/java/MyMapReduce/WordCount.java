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

/*
    --The program should ask the user for the absolute path of the directory where documents are stored. Only files ending in .txt should be considered.
    --The read function must return a stream of pairs (fileName, contents), where filename is the name of the text file and contents is a list of strings, one for each line of the file. For the read function you can exploit the enclosed class Reader.java in the way you prefer.
    --The map function must take as input the output of read and must return a stream of pairs containing, for each word (of length greater than 3) in a line, the pair (w, k) where k is the number of occurrences of w in that line.
    --The compare function should compare strings according to the standard alphanumeric ordering. (The result should adhere to the standard Java conventions, see the compareTo method of interface Comparable.)
    --The reduce function takes as input a stream of pairs (w, lst) where w is a string and lst is a list of integers. It returns a corresponding stream of pairs (w, sum) where sum is the sum of the integers in lst.
    The write function takes as input the output of reduce and writes the stream in a CSV (Comma Separated Value) file, one pair per line, in alphanumeric ordering. For the write function you can exploit the enclosed class Writer.java in the way you prefer.
* */

public class WordCount extends MapReduce<String, List<String>, String, Integer> {

    private String pathToTxtDir;

    WordCount(String directory) {
        this.pathToTxtDir = directory;
    }

    public static void main(String[] args) {
        String pathToTxtDir = getDirectory();
        new WordCount(pathToTxtDir).run();
    }

    private static String getDirectory() {
        System.out.println("Please, define the path for the directory that stores the txt files. (Press enter for default lookup in current directory ./Books/)");
        Scanner sc = new Scanner(System.in);
        String pathToTxtDir = sc.nextLine();
        if (pathToTxtDir.equals("")) {
            // TODO set ./Books instead
            pathToTxtDir = "/home/dbara/code/MapReduceFramework/src/main/resources/Books/";
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
        // TODO write output to current dir
        File outputFile = new File("/home/dbara/code/MapReduceFramework/src/main/resources/outputFile.txt");
        try {
            Writer.write(outputFile, dataToWrite);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

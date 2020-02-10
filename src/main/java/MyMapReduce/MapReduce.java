package MyMapReduce;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class MapReduce<KI, VI, KO, VO> {

    public void run() {
        List<Pair<KO, VO>> sortedDataAsList = read().flatMap(this::map)
                .sorted(Comparator.comparing(i -> i.key, this::compare))
                .collect(Collectors.toList());

        HashMap<KO, VO> reducedData = groupByKeyAndReduce(sortedDataAsList);

        Stream<Pair<KO, VO>> reducedDataAsStream = reducedData.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue()));

        write(reducedDataAsStream);
    }

    private HashMap<KO, VO> groupByKeyAndReduce(List<Pair<KO, VO>> sortedDataAsList) {
        KO currentKey = null;
        VO currentAcc = null;
        HashMap<KO, VO> map = new HashMap<>();
        int size = sortedDataAsList.size();
        for (Pair<KO, VO> pair : sortedDataAsList) {
            if (currentKey == null) {
                currentKey = pair.key;
                currentAcc = pair.value;
            }
            if (currentKey != null && compare(currentKey, pair.key) != 0) {
                map.put(currentKey, currentAcc);
                currentKey = pair.key;
                currentAcc = pair.value;
            } else {
                currentAcc = reduce(currentAcc, pair.value);
            }
            if (size == 1) {
                map.put(currentKey, currentAcc);
            }
            size--;
        }
        return map;
    }

    abstract Stream<Pair<KI, VI>> read() ;

    abstract Stream<Pair<KO, VO>> map(Pair<KI, VI> input);

    abstract int compare(KO k1, KO k2);

    abstract VO reduce(VO p1, VO p2);

    abstract void write(Stream<Pair<KO, VO>> dataToWrite);
}

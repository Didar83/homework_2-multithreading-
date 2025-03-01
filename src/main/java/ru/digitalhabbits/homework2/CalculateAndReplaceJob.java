
package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Callable;

public class CalculateAndReplaceJob implements Callable<Pair<String, Integer>> {
    private LinkedList<String> lineList;
    private LineCounterProcessor lineCounterProcessor;
    public CalculateAndReplaceJob(LinkedList<String> lineList) {
        this.lineList = lineList;
        lineCounterProcessor = new LineCounterProcessor();
    }

    @Override
    public Pair<String, Integer> call() throws Exception {
        return lineCounterProcessor.process(lineList.removeFirst());
    }
}

package ru.digitalhabbits.homework2;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        // TODO: NotImplemented: запускаем FileWriter в отдельном потоке
        final File resultFile = new File(resultFileName);
        var fileWriterThread = new Thread(new FileWriter());
        fileWriterThread.start();

        var executorService = Executors.newFixedThreadPool(CHUNK_SIZE);
        StringBuilder stringBuilder = new StringBuilder();
        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                // TODO: NotImplemented: вычитываем CHUNK_SIZE строк для параллельной обработки
                var lineList = new LinkedList<String>();
                var convertedToPairList = new LinkedList<Pair<String, Integer>>();
                for (int i = 0; i < CHUNK_SIZE; i++) {
                    if (scanner.hasNext()) {
                        lineList.addLast(scanner.nextLine());
                    }
                }
                // TODO: NotImplemented: обрабатывать строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                transferLineToPair(lineList, stringBuilder, executorService, convertedToPairList);
                // TODO: NotImplemented: добавить обработанные данные в результирующий файл
                IOUtils.write(stringBuilder.toString(), new FileOutputStream(resultFile), defaultCharset());

            }
        } catch (ExecutionException e) {
            logger.error("Error occurred during extract value of Future inside FileProcessor.process()", e);
        } catch (InterruptedException exception) {
            logger.error("InterruptedException occurred", exception);
        } catch (IOException exception) {
            logger.error("IOException occurred", exception);
        }

        // TODO: NotImplemented: остановить поток writerThread
        executorService.shutdown();
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void transferLineToPair(LinkedList<String> lineList,
                                    StringBuilder stringBuilder,
                                    ExecutorService executorService,
                                    LinkedList<Pair<String, Integer>> convertedToPairList) throws ExecutionException, InterruptedException {
        while (lineList.size() > 0) {
            Future<Pair<String, Integer>> calculatedLine = executorService.submit(new CalculateAndReplaceJob(lineList));
            convertedToPairList.add(calculatedLine.get());
            Pair<String, Integer> pairToWrite = convertedToPairList.removeLast();
            stringBuilder.append(pairToWrite.getLeft() + " " + pairToWrite.getRight()+System.lineSeparator());
        }
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}

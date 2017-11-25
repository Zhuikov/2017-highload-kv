package ru.mail.polis.zhuikov;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;

/**
 * Created by artem on 10/7/17.
 */
public class SomeDao implements Dao {

    @NotNull
    private final File dir;

    public SomeDao(@NotNull final File dir) {
        this.dir = dir;
    }

    @NotNull
    private File getFile(@NotNull final String key) {
        return new File(dir, key);
    }

    @NotNull
    @Override
    public byte[] getData(@NotNull String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final File file = getFile(key);
        if (!file.exists()) {
            throw new NoSuchElementException("unknown ID " + key);
        }
        final int fileLength = (int) file.length();
        final byte[] data = new byte[fileLength];
        if (fileLength == 0) {
            return data;
        }

        try (BufferedInputStream stream =
                     new BufferedInputStream(new FileInputStream(file))) {
            while (stream.read(data) != -1);
        }
        return data;
    }

    @NotNull
    @Override
    public void upsertData(@NotNull String key, @NotNull byte[] data) throws IllegalArgumentException, IOException {
        try (OutputStream os = new FileOutputStream(getFile(key))) {
            os.write(data);
        }
    }

    @NotNull
    @Override
    public void deleteData(@NotNull String key) throws IOException, IllegalArgumentException {
        getFile(key).delete();
    }
}

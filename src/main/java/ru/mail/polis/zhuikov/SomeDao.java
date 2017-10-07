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

    @NotNull
    private Set<String> keys = new LinkedHashSet<>();

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
        if (!keys.contains(key)) {
            throw new NoSuchElementException();
        }
        final File file = getFile(key);
        final byte[] data = new byte[(int) file.length()];
        try (InputStream is = new FileInputStream(file)) {
            if (is.read(data) != file.length()) {
                throw new IOException("Can't read the file");
            }
        }
        return data;
    }

    @NotNull
    @Override
    public void upsertData(@NotNull String key, @NotNull byte[] data) throws IllegalArgumentException, IOException {
        try (OutputStream os = new FileOutputStream(getFile(key))) {
            os.write(data);
        }
        keys.add(key);
    }

    @NotNull
    @Override
    public void deleteData(@NotNull String key) throws IOException, IllegalArgumentException {
        getFile(key).delete();
        keys.remove(key);
    }
}

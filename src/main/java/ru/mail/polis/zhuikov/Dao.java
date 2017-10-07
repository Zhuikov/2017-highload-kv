package ru.mail.polis.zhuikov;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by artem on 10/7/17.
 */
public interface Dao {

    @NotNull
    byte[] getData(@NotNull String key) throws NoSuchElementException, IllegalArgumentException, IOException;

    @NotNull
    void upsertData(@NotNull String key, @NotNull byte[] data) throws IllegalArgumentException, IOException;

    @NotNull
    void deleteData(@NotNull String key) throws IOException, IllegalArgumentException;
}

package ru.mail.polis.zhuikov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

/**
 * Created by artem on 10/7/17.
 */
public class Service implements KVService {

    @NotNull
    private final HttpServer server;
    @NotNull
    private final Dao serverDao;

    private static final String queryPrefix = "id=";
    private static final int bufferSize = 1024;

    private static String getRequestId(@NotNull final String query) {
        if (!query.startsWith(queryPrefix)) {
            throw new IllegalArgumentException("bad query");
        }

        return query.substring(queryPrefix.length());
    }

    public Service(int port, @NotNull Dao dao) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        serverDao = dao;

        server.createContext("/v0/status", httpExchange -> {
            String response = "online";
            httpExchange.sendResponseHeaders(200, response.length());
            httpExchange.getResponseBody().write(response.getBytes());
            httpExchange.close();
        });

        server.createContext("/v0/entity", httpExchange -> {

            final String id = getRequestId(httpExchange.getRequestURI().getQuery());

            if (id.isEmpty()) {
                httpExchange.sendResponseHeaders(400, 0);
                httpExchange.close();
                return;
            }

            switch (httpExchange.getRequestMethod()) {
                case "GET":
                    try {
                        final byte[] getData = serverDao.getData(id);
                        httpExchange.sendResponseHeaders(200, getData.length);
                        httpExchange.getResponseBody().write(getData);
                    } catch (NoSuchElementException e) {
                        httpExchange.sendResponseHeaders(404, 0);
                    }
                    break;
                case "DELETE":
                    serverDao.deleteData(id);
                    httpExchange.sendResponseHeaders(202, 0);
                    break;
                case "PUT":
                    InputStream requestStream = httpExchange.getRequestBody();
                    byte[] putData = new byte[bufferSize];
                    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                        while (requestStream.read(putData) != -1) {
                            stream.write(putData);
                        }
                        serverDao.upsertData(id, stream.toByteArray());
                    }
                    httpExchange.sendResponseHeaders(201, 0);
                    break;
                default:
                    httpExchange.sendResponseHeaders(405, 0);
           }

           httpExchange.close();
        });

    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}

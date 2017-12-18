package ru.mail.polis.zhuikov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

/**
 * Created by artem on 10/7/17.
 */
public class Service implements KVService {

    static byte[] readData(@NotNull InputStream is) throws IOException {

        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[BUFFER_SIZE];

            for (int len; (len = is.read(buffer, 0, BUFFER_SIZE)) != -1; ) {
                os.write(buffer, 0, len);
            }

            os.flush();
            return os.toByteArray();
        }
    }

    private static final String QUERY_ID = "id";
    private static final String QUERY_REPLICAS = "replicas";
    private static final String URL_STATUS = "/v0/status";
    private static final String URL_ENTITY = "/v0/entity";
    private static final String URL_INNER = "/v0/inner";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String URL_SERVER = "http://localhost";

    private final int PORT;
    private static final int BUFFER_SIZE = 1024;

    @NotNull
    private final HttpServer server;
    @NotNull
    private final Dao serverDao;
    @NotNull
    private final List<String> topology;

    public Service(int port, @NotNull Dao dao, @NotNull Set<String> topology) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        serverDao = dao;
        this.topology = new ArrayList<>(topology);
        PORT = port;


        server.createContext(URL_STATUS, this::statusContext);
        server.createContext(URL_ENTITY, this::entityContext);
        server.createContext(URL_INNER, this::innerContext);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }


    private void statusContext(@NotNull HttpExchange httpExchange) throws IOException {
        sendResponse(httpExchange, new Response(Response.OK, "online".getBytes()));
    }

    private void innerContext(@NotNull HttpExchange httpExchange) throws IOException {

        QueryParams params;
        try {
            params = parseQuery(httpExchange.getRequestURI().getQuery());
        } catch (IllegalArgumentException e) {
            sendResponse(httpExchange, new Response(Response.BAD_REQUEST));
            return;
        }

        if (params.getId().isEmpty() || params.getAck() > params.getFrom() || params.getAck() <= 0) {
            sendResponse(httpExchange, new Response(Response.BAD_REQUEST));
            return;
        }

        switch (httpExchange.getRequestMethod()) {
            case GET_METHOD:
                try {
                    final byte[] getData = serverDao.getData(params.getId());
                    sendResponse(httpExchange, new Response(Response.OK, getData));
                } catch (NoSuchElementException e) {
                    sendResponse(httpExchange, new Response(Response.NOT_FOUND));
                }
                break;
            case DELETE_METHOD:
                serverDao.deleteData(params.getId());
                sendResponse(httpExchange, new Response(Response.ACCEPTED));
                break;
            case PUT_METHOD:
                try {
                    InputStream requestStream = httpExchange.getRequestBody();
                    byte[] putData = readData(requestStream);
                    serverDao.upsertData(params.getId(), putData);
                    sendResponse(httpExchange, new Response(Response.CREATED, putData));
                } catch (IOException e) {
                    sendResponse(httpExchange, new Response(Response.BAD_REQUEST));
                }
                break;
            default:
                sendResponse(httpExchange, new Response(Response.NOT_ALLOWED));
        }

    }

    private void entityContext(@NotNull HttpExchange httpExchange) throws IOException {

        QueryParams params;
        try {
            params = parseQuery(httpExchange.getRequestURI().getQuery());
        } catch (IllegalArgumentException e) {
            sendResponse(httpExchange, new Response(Response.BAD_REQUEST));
            return;
        }
        if (params.getId().isEmpty() || params.getAck() > params.getFrom() || params.getAck() <= 0) {
            sendResponse(httpExchange, new Response(Response.BAD_REQUEST));
            return;
        }

        Response response;
        switch (httpExchange.getRequestMethod()) {
            case GET_METHOD:
                response =  entityGet(params);
                sendResponse(httpExchange, response);
                break;
            case DELETE_METHOD:
                response = entityDelete(params);
                sendResponse(httpExchange, response);
                break;
            case PUT_METHOD:
                response = entityPut(params, readData(httpExchange.getRequestBody()));
                sendResponse(httpExchange, response);
                break;
            default:
                sendResponse(httpExchange, new Response(Response.NOT_ALLOWED));
        }
    }

    private Response entityGet(QueryParams params) {

        int ack = 0;
        int noData = 0;
        byte[] data = null;
        for (String nodeURL : getNodes(params)) {
            Response response;
            if (nodeURL.equals(URL_SERVER + ":" + PORT)) {
                response = makeYourselfRequest(GET_METHOD, params.getId(), null);
            } else {
                response = makeNodeRequest(nodeURL, GET_METHOD, params.getId(), null);
            }
            if (response.getCode() == Response.OK) {
                ack ++;
                if (data == null) {
                    data = response.getData();
                }
            } else if (response.getCode() == Response.NOT_FOUND) {
                noData ++;
            }
        }

        boolean localData = makeYourselfRequest(GET_METHOD, params.getId(), null).getCode() == Response.OK;
        if (ack > 0 && noData == 1 && !localData) {
            makeYourselfRequest(PUT_METHOD, params.getId(), data);
            noData--;
            ack++;
        }

        if (ack + noData < params.getAck()) {
            return new Response(Response.NOT_ENOUGH_REPLICAS);
        } else if (ack < params.getAck()) {
            return new Response(Response.NOT_FOUND);
        } else {
            return new Response(Response.OK, data);
        }
    }

    private Response entityDelete(QueryParams params) {

        int ack = 0;
        for (String nodeURL : getNodes(params)) {
            Response response;
            if (nodeURL.equals(URL_SERVER + ":" + PORT)) {
                response = makeYourselfRequest(DELETE_METHOD, params.getId(), null);
            } else {
                response = makeNodeRequest(nodeURL, DELETE_METHOD, params.getId(), null);
            }
            if (response.getCode() == Response.ACCEPTED) {
                ack ++;
            }
        }

        if (ack >= params.getAck()) {
            return new Response(Response.ACCEPTED);
        }
        return new Response(Response.NOT_ENOUGH_REPLICAS);
    }

    private Response entityPut(QueryParams params, byte[] data) {

        int ack = 0;
        for (String nodeURL : getNodes(params)) {
            Response response;
            if (nodeURL.equals(URL_SERVER + ":" + PORT)) {
                response = makeYourselfRequest(PUT_METHOD, params.getId(), data);
            } else {
                response = makeNodeRequest(nodeURL, PUT_METHOD, params.getId(), data);
            }
            if (response.getCode() == Response.CREATED) {
                ack ++;
            }
        }

        if (ack >= params.getAck()) {
            return new Response(Response.CREATED);
        }

        return new Response(Response.NOT_ENOUGH_REPLICAS);
    }

    private Response makeNodeRequest(@NotNull String nodeURL, String method, String id, byte[] data) {

        HttpURLConnection connection = null;
        try {
            URL url = new URL(nodeURL + URL_INNER + "?id=" + id);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.connect();

            if (method.equals(PUT_METHOD)) {
                connection.getOutputStream().write(data);
                connection.getOutputStream().flush();
                connection.getOutputStream().close();
            }

            int responseCode = connection.getResponseCode();
            if (method.equals(GET_METHOD) && responseCode == Response.OK) {
                InputStream inputStream = connection.getInputStream();
                byte[] inputData = readData(inputStream);
                return new Response(responseCode, inputData);
            }

            return new Response(responseCode);
        } catch (IOException e) {
            return new Response(Response.SERVER_ERROR);
        } finally {
            if (null != connection) connection.disconnect();
        }
    }

    private Response makeYourselfRequest(String method, String id, byte[] data) {

        switch (method) {
            case DELETE_METHOD :
                try {
                    serverDao.deleteData(id);
                    return new Response(Response.ACCEPTED);
                } catch (IllegalArgumentException | IOException e) {
                    return new Response(Response.BAD_REQUEST);
                }
            case GET_METHOD :
                try {
                    final byte[] getData = serverDao.getData(id);
                    return new Response(Response.OK, getData);
                } catch (NoSuchElementException e) {
                    return new Response(Response.NOT_FOUND);
                } catch (IllegalArgumentException | IOException e) {
                    return new Response(Response.BAD_REQUEST);
                }
            case PUT_METHOD :
                try {
                    serverDao.upsertData(id, data);
                    return new Response(Response.CREATED);
                } catch (IOException | IllegalArgumentException e) {
                    return new Response(Response.BAD_REQUEST);
                }
            default:
                return new Response(Response.BAD_REQUEST);
        }
    }

    private void sendResponse(@NotNull HttpExchange httpExchange, @NotNull Response resp) throws IOException {

        if (resp.hasData()) {
            httpExchange.sendResponseHeaders(resp.getCode(), resp.getData().length);
            httpExchange.getResponseBody().write(resp.getData());
        } else {
            httpExchange.sendResponseHeaders(resp.getCode(), 0);
        }
        httpExchange.close();
    }

    private List<String> getNodes(@NotNull QueryParams params) {

        int hash = Math.abs(params.getId().hashCode());
        List<String> nodes = new ArrayList<>();

        for (int i = 0; i < params.getFrom(); i++) {
            int idx = (hash + i) % topology.size();
            nodes.add(topology.get(idx));
        }

        return nodes;
    }

    @NotNull
    private QueryParams parseQuery(@NotNull String query) {

        Map<String, String> params = parseParams(query);
        String id = params.get(QUERY_ID);
        int ack;
        int from;
        if (params.containsKey(QUERY_REPLICAS)) {
            String replicasParams[] = params.get(QUERY_REPLICAS).split("/");
            ack = Integer.valueOf(replicasParams[0]);
            from = Integer.valueOf(replicasParams[1]);
        } else {
            ack = topology.size() / 2 + 1;
            from = topology.size();
        }

        return new QueryParams(id, ack, from);
    }

    @NotNull
    private Map<String, String> parseParams(@NotNull String query) {

        Map<String, String> params = new LinkedHashMap<>();

        try {
            for (String param : query.split("&")) {
                int idx = param.indexOf("=");
                params.put(URLDecoder.decode(param.substring(0, idx), "UTF-8"),
                        URLDecoder.decode(param.substring(idx + 1), "UTF-8"));
            }
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Query is invalid");
        }
        return params;
    }

}

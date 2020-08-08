package org.java.plus.dag.core.base.utils.http;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.java.plus.dag.core.base.constants.TppCounterNames;
import org.java.plus.dag.core.base.exception.StatusType;
import org.java.plus.dag.core.base.utils.Debugger;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author
 */
public class HttpRequestUtils {
    private static final Logger logger = LoggerFactory.getLogger(HttpRequestUtils.class);
    private CloseableHttpClient httpClient;
    private static HttpRequestUtils instance = new HttpRequestUtils();
    private HttpRequestUtils() {
        SSLConnectionSocketFactory socketFactory = null;
        try {
            // 在调用SSL之前需要重写验证方法，取消检测SSL
            X509TrustManager trustManager = new X509TrustManager() {
                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                @Override
                public void checkClientTrusted(X509Certificate[] xcs, String str) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] xcs, String str) {
                }
            };
            SSLContext ctx = SSLContext.getInstance(SSLConnectionSocketFactory.TLS);
            ctx.init(null, new TrustManager[]{trustManager}, null);
            socketFactory = new SSLConnectionSocketFactory(ctx, NoopHostnameVerifier.INSTANCE);
        } catch (Exception e) {
            logger.error("construct socketFactory exception error:", e);
        }
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", socketFactory)
                .build();
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(500)
                .setConnectTimeout(500)
                .setConnectionRequestTimeout(500)
                .build();
        connManager.setMaxTotal(3000);
        connManager.setDefaultMaxPerRoute(2000);
        httpClient = HttpClients.custom().setConnectionManager(connManager).setDefaultRequestConfig(
                defaultRequestConfig)
                .setDefaultSocketConfig(SocketConfig.custom().setSoKeepAlive(true).setSoTimeout(5000).build()).build();
    }

    public static HttpRequestUtils getInstance() {
        return instance;
    }

    public JSONObject httpPostJSON(String url, JSON jsonParam, Integer connectionRequestTimeout, Integer connectTimeout,
                                   Integer socketTimeout) {
        JSONObject jsonResult = null;
        try {
            if (Debugger.isLocal()) {
                org.java.plus.dag.core.base.utils.Logger
                    .info(() -> "Request HTTP:\n" + url + ",params:" + String.valueOf(jsonParam));
            }
            HttpPost method = new HttpPost(url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(socketTimeout)
                    .setConnectTimeout(connectTimeout)
                    .setConnectionRequestTimeout(connectionRequestTimeout)
                    .build();

            method.setConfig(requestConfig);
            if (null != jsonParam) {
                StringEntity entity = new StringEntity(jsonParam.toString(), "utf-8");
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");
                method.setEntity(entity);
            }
            jsonResult = httpClient.execute(method, new HttpResponseJSONHandler());
        } catch (Exception e) {
            Debugger.exception(HttpRequestUtils.class, StatusType.INVOKE_HTTP_EXCEPTION, e);
            logger.error("post method exception error , url is :" + url, e);
        }
        return jsonResult;
    }


    public JSONObject httpPostJSON(String url, JSON jsonParam) {
        JSONObject jsonResult = null;
        HttpPost method = new HttpPost(url);
        try {
            if (null != jsonParam) {
                StringEntity entity = new StringEntity(jsonParam.toString(), "utf-8");
                entity.setContentEncoding("UTF-8");
                entity.setContentType("application/json");
                method.setEntity(entity);
            }
            jsonResult = httpClient.execute(method, new HttpResponseJSONHandler());
        } catch (Exception e) {
            Debugger.exception(HttpRequestUtils.class, StatusType.INVOKE_HTTP_EXCEPTION, e);
            logger.error("post method exception error , url is :" + url, e);
        }
        return jsonResult;
    }


    public JSONObject httpGetJSON(String url){
        JSONObject jsonResult =null;
        try {
            HttpGet method = new HttpGet(url);
            jsonResult = httpClient.execute(method, new HttpResponseJSONHandler() );
        } catch (Exception e) {
            logger.error("get method exception error , url is :" + url, e);
        }
        return jsonResult;
    }


    class HttpResponseJSONHandler implements ResponseHandler<JSONObject> {
        @Override
        public JSONObject handleResponse(HttpResponse httpResponse) throws ClientProtocolException, IOException {
            JSONObject jsonResult = new JSONObject();
            int status = httpResponse.getStatusLine().getStatusCode();
            if (status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES) {
                try {
                    InputStream in = httpResponse.getEntity().getContent();
                    String str = IOUtils.toString(in, "UTF-8");
                    jsonResult = JSON.parseObject(str);
                    in.close();
                } catch (Exception e) {
                    logger.error("post parse response json error :" + e);
                }
            } else {
                logger.error("Unexpected http response status: " + status);
                jsonResult.put(TppCounterNames.UC_AD_INVOKE_FAILURE.getCounterName(), status);
            }
            return jsonResult;
        }
    }

}
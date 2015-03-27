package com.strategicgains.docussandra;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.restexpress.RestExpress;
import org.restexpress.exception.BadRequestException;
import org.restexpress.exception.ConflictException;
import org.restexpress.exception.NotFoundException;
import org.restexpress.pipeline.SimpleConsoleLogMessageObserver;
import org.restexpress.plugin.hyperexpress.HyperExpressPlugin;
import org.restexpress.plugin.hyperexpress.Linkable;
import org.restexpress.plugin.version.VersionPlugin;
import org.restexpress.util.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.strategicgains.docussandra.cache.CacheFactory;
import com.strategicgains.docussandra.config.Configuration;
import com.strategicgains.docussandra.serialization.SerializationProvider;
import com.strategicgains.repoexpress.adapter.Identifiers;
import com.strategicgains.repoexpress.exception.DuplicateItemException;
import com.strategicgains.repoexpress.exception.InvalidObjectIdException;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.restexpress.plugin.metrics.MetricsConfig;
import com.strategicgains.restexpress.plugin.metrics.MetricsPlugin;
import com.strategicgains.syntaxe.ValidationException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main
{

    private static final String SERVICE_NAME = "Docussandra API";
    private static final Logger LOG = LoggerFactory.getLogger(SERVICE_NAME);

    public static void main(String[] args) throws Exception
    {
        RestExpress server = initializeServer(args);
        LOG.info("Server started up!");
        server.awaitShutdown();
         Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                LOG.info("Shutting down Docussandra...");
                CacheFactory.shutdownCacheManger();
            }
        }, "Shutdown-thread"));
    }

    public static RestExpress initializeServer(String[] args) throws IOException
    {
        RestExpress.setSerializationProvider(new SerializationProvider());
        Identifiers.UUID.useShortUUID(true);

        Configuration config = loadEnvironment(args);
        RestExpress server = new RestExpress()
                .setName(config.getProjectName(SERVICE_NAME))
                .setBaseUrl(config.getBaseUrl())
                .setExecutorThreadCount(config.getExecutorThreadPoolSize())
                .addMessageObserver(new SimpleConsoleLogMessageObserver());

        new VersionPlugin(config.getProjectVersion())
                .register(server);

        Routes.define(config, server);
        Relationships.define(server);
        configurePlugins(config, server);
        mapExceptions(server);
        server.bind(config.getPort());
        return server;
    }

    private static void configurePlugins(Configuration config, RestExpress server)
    {
        configureMetrics(config, server);

        new HyperExpressPlugin(Linkable.class)
                .register(server);
    }

    private static void configureMetrics(Configuration config, RestExpress server)
    {
        MetricsConfig mc = config.getMetricsConfig();

        if (mc.isEnabled())
        {
            MetricRegistry registry = new MetricRegistry();
            new MetricsPlugin(registry)
                    .register(server);

            if (mc.isGraphiteEnabled())
            {
                final Graphite graphite = new Graphite(new InetSocketAddress(mc.getGraphiteHost(), mc.getGraphitePort()));
                final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                        .prefixedWith(mc.getPrefix())
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .filter(MetricFilter.ALL)
                        .build(graphite);
                reporter.start(mc.getPublishSeconds(), TimeUnit.SECONDS);
            } else
            {
                LOG.warn("*** Graphite Metrics Publishing is Disabled ***");
            }
        } else
        {
            LOG.warn("*** Metrics Generation is Disabled ***");
        }
    }

    private static void mapExceptions(RestExpress server)
    {
        server
                .mapException(ItemNotFoundException.class, NotFoundException.class)
                .mapException(DuplicateItemException.class, ConflictException.class)
                .mapException(ValidationException.class, BadRequestException.class)
                .mapException(InvalidObjectIdException.class, BadRequestException.class);
    }

    private static Configuration loadEnvironment(String[] args)
            throws FileNotFoundException, IOException
    {
        LOG.info("Loading environment with " + args.length + " arguments.");
        if (args.length > 0)
        {
            LOG.info("-args[0]: " + args[0]);
            if (args[0].startsWith("http") || args[0].startsWith("HTTP"))//if we are fetching props by URL
            {
                Configuration config = new Configuration();
                config.fillValues(fetchPropertiesFromServer(args[0]));
                return config;
            } else //load from standard config
            {
                return Environment.from(args[0], Configuration.class);
            }
        }
        return Environment.fromDefault(Configuration.class);
    }

    private static Properties fetchPropertiesFromServer(String url)
    {
        Properties properties = new Properties();
        if (url != null)
        {
            HttpClient client = HttpClientBuilder.create().build();
            JSONParser parser = new JSONParser();
            HttpGet request = new HttpGet(url);
            RequestConfig rc = rc = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(3600000).setConnectionRequestTimeout(60000).build();;
            request.setConfig(rc);
            // add request header
            request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
//            //add auth if specified
//            if (authToken != null)
//            {
//                request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authToken);
//            }
            BufferedReader rd = null;
            InputStreamReader isr = null;
            try
            {
                HttpResponse response = client.execute(request);
                int responseCode = response.getStatusLine().getStatusCode();
                if (responseCode != 200)
                {
                    throw new RuntimeException("Cannot fetch properties: Error when doing a GET call agaist: " + url + ". Error code: " + responseCode + " Status code: " + response.getStatusLine().getStatusCode());
                }
                isr = new InputStreamReader(response.getEntity().getContent());
                rd = new BufferedReader(isr);
                properties.putAll((JSONObject) parser.parse(rd));
            } catch (ParseException pe)
            {
                throw new RuntimeException("Cannot fetch properties: Could not parse JSON", pe);
            } catch (IOException e)
            {
                throw new RuntimeException("Cannot fetch properties: Problem contacting REST service for GET, URL: " + url, e);
            } finally
            {
                if (rd != null)
                {
                    try
                    {
                        rd.close();
                    } catch (IOException e)
                    {
                        LOG.debug("Could not close BufferedReader...", e);
                    }
                }
                if (isr != null)
                {
                    try
                    {
                        isr.close();
                    } catch (IOException e)
                    {
                        LOG.debug("Could not close InputStreamReader...", e);
                    }
                }
                request.reset();
            }
        }
        return properties;
    }
}

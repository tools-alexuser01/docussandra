package com.strategicgains.mongossandra.config;

import java.util.Properties;

import org.restexpress.RestExpress;
import org.restexpress.util.Environment;

import com.strategicgains.mongossandra.controller.CollectionsController;
import com.strategicgains.mongossandra.controller.EntitiesController;
import com.strategicgains.mongossandra.controller.IndexesController;
import com.strategicgains.mongossandra.controller.NamespacesController;
import com.strategicgains.mongossandra.controller.QueriesController;
import com.strategicgains.mongossandra.persistence.NamespacesRepository;
import com.strategicgains.mongossandra.service.NamespacesService;
import com.strategicgains.repoexpress.cassandra.CassandraConfig;

public class Configuration
extends Environment
{
	private static final String DEFAULT_EXECUTOR_THREAD_POOL_SIZE = "20";

	private static final String PORT_PROPERTY = "port";
	private static final String BASE_URL_PROPERTY = "base.url";
	private static final String EXECUTOR_THREAD_POOL_SIZE = "executor.threadPool.size";

	private int port;
	private String baseUrl;
	private int executorThreadPoolSize;
	private MetricsConfig metricsSettings;

	private NamespacesController namespacesController;
	private CollectionsController collectionsController;
	private EntitiesController entitiesController;
	private IndexesController indexesController;
	private QueriesController queriesController;

	@Override
	protected void fillValues(Properties p)
	{
		this.port = Integer.parseInt(p.getProperty(PORT_PROPERTY, String.valueOf(RestExpress.DEFAULT_PORT)));
		this.baseUrl = p.getProperty(BASE_URL_PROPERTY, "http://localhost:" + String.valueOf(port));
		this.executorThreadPoolSize = Integer.parseInt(p.getProperty(EXECUTOR_THREAD_POOL_SIZE, DEFAULT_EXECUTOR_THREAD_POOL_SIZE));
		this.metricsSettings = new MetricsConfig(p);
		CassandraConfig dbConfig = new CassandraConfig(p);
		initialize(dbConfig);
	}

	private void initialize(CassandraConfig dbConfig)
	{
		NamespacesRepository namespacesRepository = new NamespacesRepository(dbConfig.getSession());
		NamespacesService namespacesService = new NamespacesService(namespacesRepository);
		namespacesController = new NamespacesController(namespacesService);

		// TODO: create service and repository implementations for these...
//		collectionsController = new CollectionsController(SampleUuidEntityService);
//		entitiesController = new EntitiesController(SampleUuidEntityService);
//		indexesController = new IndexesController(SampleUuidEntityService);
//		queriesController = new QueriesController(SampleUuidEntityService);
	}

	public int getPort()
	{
		return port;
	}
	
	public String getBaseUrl()
	{
		return baseUrl;
	}
	
	public int getExecutorThreadPoolSize()
	{
		return executorThreadPoolSize;
	}

	public MetricsConfig getMetricsConfig()
    {
	    return metricsSettings;
    }

	public Object getNamespacesController()
    {
		return namespacesController;
    }

	public Object getCollectionsController()
    {
		return collectionsController;
    }

	public Object getEntitiesController()
    {
		return entitiesController;
    }

	public Object getIndexesController()
    {
		return indexesController;
    }

	public Object getQueryController()
    {
		return queriesController;
    }
}

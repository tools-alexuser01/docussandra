package com.strategicgains.docussandra.config;

import java.util.Properties;

import org.restexpress.RestExpress;
import org.restexpress.util.Environment;

import com.strategicgains.docussandra.controller.CollectionsController;
import com.strategicgains.docussandra.controller.DocumentsController;
import com.strategicgains.docussandra.controller.IndexesController;
import com.strategicgains.docussandra.controller.NamespacesController;
import com.strategicgains.docussandra.controller.QueriesController;
import com.strategicgains.docussandra.handler.IndexCreatedHandler;
import com.strategicgains.docussandra.handler.IndexDeletedHandler;
import com.strategicgains.docussandra.handler.NamespaceDeletedHandler;
import com.strategicgains.docussandra.persistence.CollectionsRepository;
import com.strategicgains.docussandra.persistence.DocumentsRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.NamespacesRepository;
import com.strategicgains.docussandra.persistence.QueryRepository;
import com.strategicgains.docussandra.service.CollectionsService;
import com.strategicgains.docussandra.service.DocumentsService;
import com.strategicgains.docussandra.service.IndexService;
import com.strategicgains.docussandra.service.NamespacesService;
import com.strategicgains.docussandra.service.QueryService;
import com.strategicgains.eventing.DomainEvents;
import com.strategicgains.eventing.EventBus;
import com.strategicgains.eventing.local.LocalEventBusBuilder;
import com.strategicgains.repoexpress.cassandra.CassandraConfig;
import com.strategicgains.restexpress.plugin.metrics.MetricsConfig;

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
	private DocumentsController documentsController;
	private IndexesController indexController;
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
		CollectionsRepository collectionsRepository = new CollectionsRepository(dbConfig.getSession());
		DocumentsRepository documentsRepository = new DocumentsRepository(dbConfig.getSession());
		IndexRepository indexRepository = new IndexRepository(dbConfig.getSession());
		QueryRepository queryRepository = new QueryRepository(dbConfig.getSession());

		NamespacesService namespacesService = new NamespacesService(namespacesRepository);
		CollectionsService collectionsService = new CollectionsService(namespacesRepository, collectionsRepository);
		DocumentsService documentsService = new DocumentsService(collectionsRepository, documentsRepository);
		IndexService indexService = new IndexService(collectionsRepository, indexRepository);
		QueryService queryService = new QueryService(queryRepository);

		namespacesController = new NamespacesController(namespacesService);
		collectionsController = new CollectionsController(collectionsService);
		documentsController = new DocumentsController(documentsService);
		indexController = new IndexesController(indexService);
		queriesController = new QueriesController(queryService);

		// TODO: create service and repository implementations for these...
//		entitiesController = new EntitiesController(SampleUuidEntityService);

		EventBus bus = new LocalEventBusBuilder()
			.subscribe(new IndexCreatedHandler())
			.subscribe(new IndexDeletedHandler())
			.subscribe(new NamespaceDeletedHandler())
			.build();
		DomainEvents.addBus("local", bus);

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

	public NamespacesController getNamespacesController()
    {
		return namespacesController;
    }

	public CollectionsController getCollectionsController()
    {
		return collectionsController;
    }

	public DocumentsController getDocumentsController()
    {
		return documentsController;
    }

	public IndexesController getIndexesController()
    {
		return indexController;
    }

	public QueriesController getQueryController()
    {
		return queriesController;
    }
}

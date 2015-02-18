package com.strategicgains.docussandra.config;

import java.util.Properties;

import org.restexpress.RestExpress;
import org.restexpress.util.Environment;

import com.strategicgains.docussandra.controller.DatabaseController;
import com.strategicgains.docussandra.controller.DocumentController;
import com.strategicgains.docussandra.controller.IndexController;
import com.strategicgains.docussandra.controller.QueryController;
import com.strategicgains.docussandra.controller.TableController;
import com.strategicgains.docussandra.handler.DatabaseDeletedHandler;
import com.strategicgains.docussandra.handler.IndexCreatedHandler;
import com.strategicgains.docussandra.handler.IndexDeletedHandler;
import com.strategicgains.docussandra.persistence.DatabaseRepository;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import com.strategicgains.docussandra.persistence.IndexRepository;
import com.strategicgains.docussandra.persistence.QueryDao;
import com.strategicgains.docussandra.persistence.TableRepository;
import com.strategicgains.docussandra.service.DatabaseService;
import com.strategicgains.docussandra.service.DocumentService;
import com.strategicgains.docussandra.service.IndexService;
import com.strategicgains.docussandra.service.QueryService;
import com.strategicgains.docussandra.service.TableService;
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

	private DatabaseController databaseController;
	private TableController tableController;
	private DocumentController documentController;
	private IndexController indexController;
	private QueryController queryController;

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
		DatabaseRepository databaseRepository = new DatabaseRepository(dbConfig.getSession());
		TableRepository tableRepository = new TableRepository(dbConfig.getSession());
		DocumentRepository documentRepository = new DocumentRepository(dbConfig.getSession());
		IndexRepository indexRepository = new IndexRepository(dbConfig.getSession());
		QueryDao queryRepository = new QueryDao(dbConfig.getSession());

		DatabaseService databaseService = new DatabaseService(databaseRepository);
		TableService tableService = new TableService(databaseRepository, tableRepository);
		DocumentService documentService = new DocumentService(tableRepository, documentRepository);
		IndexService indexService = new IndexService(tableRepository, indexRepository);
		QueryService queryService = new QueryService(queryRepository);

		databaseController = new DatabaseController(databaseService);
		tableController = new TableController(tableService);
		documentController = new DocumentController(documentService);
		indexController = new IndexController(indexService);
		queryController = new QueryController(queryService);

		// TODO: create service and repository implementations for these...
//		entitiesController = new EntitiesController(SampleUuidEntityService);

		EventBus bus = new LocalEventBusBuilder()
			.subscribe(new IndexCreatedHandler())
			.subscribe(new IndexDeletedHandler())
			.subscribe(new DatabaseDeletedHandler())
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

	public DatabaseController getDatabaseController()
    {
		return databaseController;
    }

	public TableController getTableController()
    {
		return tableController;
    }

	public DocumentController getDocumentController()
    {
		return documentController;
    }

	public IndexController getIndexController()
    {
		return indexController;
    }

	public QueryController getQueryController()
    {
		return queryController;
    }
}

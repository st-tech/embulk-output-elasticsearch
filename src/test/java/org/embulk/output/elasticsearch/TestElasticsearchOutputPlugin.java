package org.embulk.output.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.AuthMethod;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.Mode;
import org.embulk.output.elasticsearch.ElasticsearchOutputPluginDelegate.PluginTask;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_BULK_ACTIONS;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_BULK_SIZE;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_CONCURRENT_REQUESTS;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_ID;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_INDEX_TYPE;
import static org.embulk.output.elasticsearch.ElasticsearchTestUtils.ES_NODES;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestElasticsearchOutputPlugin
{
    private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES;

    static {
        final Properties properties = new Properties();
        properties.setProperty("default_guess_plugins", "gzip,bzip2,json,csv");
        EMBULK_SYSTEM_PROPERTIES = EmbulkSystemProperties.of(properties);
    }

    @Rule
    public TestingEmbulk embulk = TestingEmbulk.builder()
            .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
            .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
            .registerPlugin(OutputPlugin.class, "elasticsearch", ElasticsearchOutputPlugin.class)
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .build();

    private ElasticsearchOutputPlugin plugin;
    private ElasticsearchTestUtils utils;

    @Before
    public void createResources() throws Exception
    {
        utils = new ElasticsearchTestUtils();
        utils.initializeConstant();
        PluginTask task = utils.config().loadConfig(PluginTask.class);
        utils.prepareBeforeTest(task);

        plugin = new ElasticsearchOutputPlugin();
    }

    /*
    @Test
    public void testDefaultValues()
    {
        final PluginTask task =
                ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.createConfigMapper().map(utils.config(), PluginTask.class);
        assertThat(task.getIndex(), is(ES_INDEX));
    }

    @Test
    public void testDefaultValuesNull()
    {
        ConfigSource config = ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newConfigSource()
            .set("in", utils.inputConfig())
            .set("parser", utils.parserConfig(utils.schemaConfig()))
            .set("type", "elasticsearch")
            .set("mode", "") // NULL
            .set("nodes", ES_NODES)
            .set("index", ES_INDEX)
            .set("index_type", ES_INDEX_TYPE)
            .set("id", ES_ID)
            .set("bulk_actions", ES_BULK_ACTIONS)
            .set("bulk_size", ES_BULK_SIZE)
            .set("concurrent_requests", ES_CONCURRENT_REQUESTS
            );
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        try {
            plugin.transaction(config, schema, 0, new OutputPlugin.Control()
            {
                @Override
                public List<TaskReport> run(TaskSource taskSource)
                {
                    return Lists.newArrayList(ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport());
                }
            });
        }
        catch (Throwable t) {
            if (t instanceof RuntimeException) {
                assertTrue(t instanceof ConfigException);
            }
        }
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = utils.config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        // no error happens
    }

    @Test
    public void testResume()
    {
        ConfigSource config = utils.config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.resume(task.dump(), schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
    }

    @Test
    public void testCleanup()
    {
        ConfigSource config = utils.config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), schema, 0, Arrays.asList(ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport()));
        // no error happens
    }

    @Test
    public void testOutputByOpen() throws Exception
    {
        ConfigSource config = utils.config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(ElasticsearchOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 1L, 32864L, Timestamp.ofEpochSecond(1422386629), Timestamp.ofEpochSecond(1422316800),  true, 123.45, "embulk");
        assertThat(pages.size(), is(1));
        for (Page page : pages) {
            output.add(page);
        }

        output.finish();
        output.commit();
        Thread.sleep(1500); // Need to wait until index done

        ElasticsearchHttpClient client = new ElasticsearchHttpClient();
        Method sendRequest = ElasticsearchHttpClient.class.getDeclaredMethod("sendRequest", String.class, HttpMethod.class, PluginTask.class, String.class);
        sendRequest.setAccessible(true);
        String path = String.format("/%s/%s/_search", ES_INDEX, ES_INDEX_TYPE);
        String sort = "{\"sort\" : \"id\"}";
        JsonNode response = (JsonNode) sendRequest.invoke(client, path, HttpMethod.POST, task, sort);
        assertThat(response.get("hits").get("total").asInt(), is(1));
        if (response.size() > 0) {
            JsonNode record = response.get("hits").get("hits").get(0).get("_source");
            assertThat(record.get("id").asInt(), is(1));
            assertThat(record.get("account").asInt(), is(32864));
            assertThat(record.get("time").asText(), is("2015-01-27T19:23:49.000+0000"));
            assertThat(record.get("purchase").asText(), is("2015-01-27T00:00:00.000+0000"));
            assertThat(record.get("flg").asBoolean(), is(true));
            assertThat(record.get("score").asDouble(), is(123.45));
            assertThat(record.get("comment").asText(), is("embulk"));
        }
    }

    @Test
    public void testOpenAbort()
    {
        ConfigSource config = utils.config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
        output.abort();
        // no error happens.
    }

    @Test
    public void testMode()
    {
        assertThat(Mode.values().length, is(2));
        assertThat(Mode.valueOf("INSERT"), is(Mode.INSERT));
    }

    @Test
    public void testAuthMethod()
    {
        assertThat(AuthMethod.values().length, is(2));
        assertThat(AuthMethod.valueOf("BASIC"), is(AuthMethod.BASIC));
    }

    @Test(expected = ConfigException.class)
    public void testModeThrowsConfigException()
    {
        Mode.fromString("non-exists-mode");
    }
    */
}

package com.keedio.kds.flink.injector.integration;
import com.keedio.kds.flink.injector.integration.AbstractElasticsearchIntegrationTest;
import org.elasticsearch.action.get.GetResponse;
import org.junit.Assert;
import org.junit.Test;


import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * Demonstrates how to use an embedded elasticsearch server in your tests.
 *
 * @author Felix Müller
 */
public class SimpleElasticsearchTest extends AbstractElasticsearchIntegrationTest {


  @Test
  public void indexSimpleDocument() throws IOException {
    getClient().prepareIndex("myindex", "document", "1")
      .setSource(jsonBuilder().startObject().field("test", "123").endObject())
      .execute()
      .actionGet();

    GetResponse fields = getClient().prepareGet("myindex", "document", "1").execute().actionGet();

    Assert.assertEquals(fields.getSource().get("test"),("123"));
  }
}

package com.dynamodemo.app;

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;

import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class App {
  private static final String TableName = "workshop";
  private static Builder AttributeBuilder = AttributeValue.builder();
  private static DynamoDbClient DynamoClient = DynamoDbClient.create();

  public static void main(String[] args) {
    // testAwsCredentials();

    var id = putItem();
    printItem(id);
    updateItem(id);
    deleteItem(id);
    printItem(id);
  }

  private static void testAwsCredentials() {
    var sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
    String account = sts.getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
    System.out.println("Account ID:" + account);
  }

  private static AttributeValue stringAttribute(String value) {
    return AttributeBuilder.s(value).build();
  }

  private static String putItem() {
    var itemValues = new HashMap<String, AttributeValue>();
    var id = UUID.randomUUID().toString();
    itemValues.put("id", stringAttribute(id));
    itemValues.put("name", stringAttribute("mochi"));

    System.out.format("\nCreating item with id: %s\n", id);

    var request = PutItemRequest.builder().tableName(TableName).item(itemValues).build();
    DynamoClient.putItem(request);
    return id;
  }

  private static void printItem(String id) {
    System.out.format("\nFetching item with id: %s\n", id);
    var keyToGet = new HashMap<String, AttributeValue>();
    keyToGet.put("id", stringAttribute(id));

    var request = GetItemRequest.builder().key(keyToGet).tableName(TableName).build();
    Map<String, AttributeValue> returnItem = DynamoClient.getItem(request).item();

    if (returnItem != null) {
      Set<String> keys = returnItem.keySet();
      for (String key : keys) {
        AttributeValue attribute = returnItem.get(key);
        // this assumes the value is a string
        System.out.format("%s: %s\n", key, attribute.s());
      }

      if (keys.isEmpty()) {
        System.out.format("Item returned no keys!\n", id);
      }
    } else {
      System.out.format("No item found with the id %s!\n", id);
    }
  }

  private static void updateItem(String id) {
    System.out.format("\nUpdating item with id: %s\n", id);
    var itemKey = new HashMap<String, AttributeValue>();
    itemKey.put("id", stringAttribute(id));

    var attributeUpdates = new HashMap<String, AttributeValueUpdate>();
    var builder = AttributeValueUpdate.builder();
    var updateValue = builder.value(stringAttribute("cat")).action(AttributeAction.PUT).build();
    attributeUpdates.put("type", updateValue);

    var request = UpdateItemRequest.builder().tableName(TableName).key(itemKey).attributeUpdates(attributeUpdates)
        .build();

    DynamoClient.updateItem(request);
  }

  private static void deleteItem(String id) {
    System.out.format("\nDeleting item with id: %s\n", id);
    var itemKey = new HashMap<String, AttributeValue>();
    itemKey.put("id", stringAttribute(id));

    var deleteItemRequest = DeleteItemRequest.builder().tableName(TableName).key(itemKey).build();
    DynamoClient.deleteItem(deleteItemRequest);
  }
}

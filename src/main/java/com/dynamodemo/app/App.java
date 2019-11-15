package com.dynamodemo.app;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;

import java.util.UUID;

public class App {

  @DynamoDBTable(tableName = "PetStore")
  public static class Pet {
    private String id;
    private String name;
    private String species;

    @DynamoDBHashKey(attributeName = "id")
    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    @DynamoDBAttribute(attributeName = "name")
    public String getName() {
      return this.name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @DynamoDBAttribute(attributeName = "species")
    public String getSpecies() {
      return this.species;
    }

    public void setSpecies(String species) {
      this.species = species;
    }
  }

  public static void main(String[] args) {
    // testAwsCredentials();
    testCRUDOperations();
  }

  private static void testAwsCredentials() {
    var sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
    String account = sts.getCallerIdentity(new GetCallerIdentityRequest()).getAccount();
    System.out.println("Account ID:" + account);
  }

  private static void testCRUDOperations() {
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    DynamoDBMapper mapper = new DynamoDBMapper(client);

    Pet p = new Pet();
    p.setId(UUID.randomUUID().toString());
    p.setName("mochi");
    p.setSpecies("cat");

    // save the item
    mapper.save(p);

    // fetch the item
    Pet retrieved = mapper.load(Pet.class, p.id);
    System.out.println("Item retrieved:");
    System.out.println(retrieved);

    // update the item
    retrieved.setName("buttons");
    mapper.save(retrieved);

    System.out.println("Item updated:");
    System.out.println(retrieved);

    // read the updated item
    DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
        .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT).build();
    Pet updatedPet = mapper.load(Pet.class, p.id, config);
    System.out.println("Retrieved the previously updated item:");
    System.out.println(updatedPet);

    // delete the item
    mapper.delete(updatedPet);

    // try to retrieve the deleted item
    Pet deletedPet = mapper.load(Pet.class, p.id, config);
    if (deletedPet == null) {
      System.out.println("Done - Sample pet deleted");
    }
  }
}

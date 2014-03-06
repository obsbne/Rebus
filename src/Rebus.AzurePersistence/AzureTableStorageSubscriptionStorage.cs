using System;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Rebus.AzurePersistence
{
    public class AzureTableStorageSubscriptionStorage : IStoreSubscriptions
    {
        const string PartitionKey = "Rebus-Subscriptions";
        readonly string connectionString;
        readonly string subscriptionTableName;

        public AzureTableStorageSubscriptionStorage(string connectionString, string subscriptionTableName)
        {
            this.connectionString = connectionString;
            this.subscriptionTableName = subscriptionTableName;
        }

        public virtual void Store(Type eventType, string subscriberInputQueue)
        {
            var table = EnsureTableStorage();

            var subscription = new RebusSubscription(eventType, subscriberInputQueue);
            var retrieveOperation = TableOperation.Retrieve<RebusSubscription>(subscription.PartitionKey, subscription.RowKey);
            var result = table.Execute(retrieveOperation);

            if (result.Result != null)
                return;

            var createSubscriptionOperation = TableOperation.Insert(subscription);
            var creationOperationResult = table.Execute(createSubscriptionOperation);

            if (creationOperationResult.Result == null)
                throw new InvalidOperationException("The subscription registration failed.");
        }

        public virtual void Remove(Type eventType, string subscriberInputQueue)
        {
            var table = EnsureTableStorage();

            var subscription = new RebusSubscription(eventType, subscriberInputQueue);
            var retrieveOperation = TableOperation.Retrieve<RebusSubscription>(subscription.PartitionKey, subscription.RowKey);
            var retrievedResult = table.Execute(retrieveOperation);
            if (retrievedResult.Result == null)
                return;

            var removeOperation = TableOperation.Delete(retrievedResult.Result as ITableEntity);
            table.Execute(removeOperation);
        }

        public virtual string[] GetSubscribers(Type eventType)
        {
            var table = EnsureTableStorage();

            var query = new TableQuery<RebusSubscription>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey",
                    QueryComparisons.Equal, Key(eventType)));

            var result = table.ExecuteQuery(query);
            return result.Select(x => x.RowKey)
                .ToArray();
        }

        private CloudTable EnsureTableStorage()
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(subscriptionTableName);
            table.CreateIfNotExists();
            return table;
        }

        static string Key(Type eventType)
        {
            return string.Format("{0}-{1}", PartitionKey, eventType.FullName);
        }

        class RebusSubscription : TableEntity
        {
            public RebusSubscription()
            {
            }

            public RebusSubscription(Type subscriptionType, string subscriberQueue)
            {
                PartitionKey = Key(subscriptionType);
                RowKey = subscriberQueue;
            }
        }
    }
}

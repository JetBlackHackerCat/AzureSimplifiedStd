using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.File;
using Microsoft.Azure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Xml;

namespace AzureSimplifiedStd.Storage
{
    enum StorageCredentialType { SAS = 0, Key = 1 };
    public class TableSegment
    {
        List<TableRow> segment;
        string continuationToken;
        public TableSegment()
        {
            segment = new List<TableRow>();
            continuationToken = string.Empty;
        }
        public TableSegment(List<TableRow> rows, string token)
        {
            segment = rows;
            continuationToken = token;
        }
        public void AddRowToSegement(DynamicTableEntity enti)
        {
            TableRow row = new TableRow(enti.RowKey, enti.PartitionKey);

            row.AddColumnAndValue("Timestamp", enti.Timestamp.ToString());
            row.AddColumnAndValue("Etag", enti.ETag);
            foreach (KeyValuePair<string, EntityProperty> pair in enti.Properties)
            {
                row.AddColumnAndValue(pair.Key, pair.Value.StringValue);
            }

            segment.Add(row);
        }
        public void AddRowToSegement(TableRow enti)
        {
            segment.Add(enti);
        }
        public string GetContinuationToken()
        {
            return continuationToken;
        }

        public void SetContinuationToken(string token)
        {
            continuationToken = token;
        }

    }

    public class TableRow
    {
        DynamicTableEntity clayTable;

        public TableRow()
        {
            clayTable = new DynamicTableEntity();
        }
        public TableRow(string rowKey, string partitionKey)
        {
            clayTable = new DynamicTableEntity(partitionKey, rowKey);
        }
        public TableRow(object dynamicTableEntity)
        {
            clayTable = (DynamicTableEntity)dynamicTableEntity;
        }

        public void AddColumnAndValue(string name, string value)
        {
            EntityProperty prop = new EntityProperty(value);
            clayTable.Properties.Add(name, prop);
        }
        public void UpdateValue(string name, string value)
        {
            EntityProperty prop = new EntityProperty(value);
            clayTable.Properties[name] = prop;
        }
        public void RemoveColumn(string name)
        {
            clayTable.Properties.Remove(name);
        }

        public Dictionary<string, string> ToDictionary()
        {
            Dictionary<string, string> temp = new Dictionary<string, string>();
            temp.Add("PartitionKey", clayTable.PartitionKey);
            temp.Add("RowKey", clayTable.RowKey);
            temp.Add("ETag", clayTable.ETag);
            temp.Add("Timestamp", clayTable.Timestamp.ToLocalTime().ToString());
            foreach (KeyValuePair<string, EntityProperty> pair in clayTable.Properties)
            {
                temp.Add(pair.Key, pair.Value.StringValue);
            }
            return temp;
        }

        public DynamicTableEntity GetEntityToUpload()
        {
            return clayTable;
        }




    }
    class StorageAccount
    {
        public class StorageAccountException : Exception
        {
            public StorageAccountException()
            {
            }

            public StorageAccountException(string message) : base(message)
            {
            }

            public StorageAccountException(string message, Exception innerException) : base(message, innerException)
            {
            }

            protected StorageAccountException(SerializationInfo info, StreamingContext context) : base(info, context)
            {
            }

        }
        private struct AsyncBox
        {
            public TableQuerySegment<DynamicTableEntity> tableQuerySegment;
            public TableContinuationToken token;
        }
        private Uri fileURI;
        private Uri queueURI;
        private Uri blobURI;
        private Uri tableURI;
        private Microsoft.Azure.Storage.Auth.StorageCredentials creds;
        private StorageCredentials tableCreds;
        //Constructors
        public StorageAccount(string storageName)
        {
            fileURI = new Uri(string.Format("https://{0}.file.core.windows.net/"));
            queueURI = new Uri(string.Format("https://{0}.queue.core.windows.net/"));
            blobURI = new Uri(string.Format("https://{0}.blob.core.windows.net/"));
            tableURI = new Uri(string.Format("https://{0}.table.core.windows.net/"));
        }
        public StorageAccount(string storageName, StorageCredentialType type, string keyOrToken)
        {
            fileURI = new Uri(string.Format("https://{0}.file.core.windows.net/"));
            queueURI = new  Uri(string.Format("https://{0}.queue.core.windows.net/"));
            blobURI = new Uri(string.Format("https://{0}.blob.core.windows.net/"));
            tableURI = new Uri(string.Format("https://{0}.table.core.windows.net/"));
            try
            {
                if (type == StorageCredentialType.SAS)
                {
                    creds = new Microsoft.Azure.Storage.Auth.StorageCredentials(keyOrToken);
                    tableCreds = new StorageCredentials(keyOrToken);
                }

                else
                {
                    creds = new Microsoft.Azure.Storage.Auth.StorageCredentials(storageName, keyOrToken);
                    tableCreds = new StorageCredentials(storageName,keyOrToken);
                }
                    

            }
            catch (Exception e)
            {
                throw new StorageAccountException("Error while attempting to authenticate", e);
            }

        }
        //File Shares
        public bool DeleteFileShareIfExists(string fileShareName)
        {
            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);
            return share.DeleteIfExists();
        }
        public bool CreateFileShareIfNotExists(string fileShareName)
        {
            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);
            return share.CreateIfNotExists();
        }
        public void UploadToFileShare(string fileShareName, string fileContents, string fileName = "")
        {
            //Create GUID to for filename if no name specified
            if (fileName.Length == 0)
            {
                fileName = Guid.NewGuid().ToString();
            }
            byte[] filebytes = Encoding.UTF8.GetBytes(fileContents);
            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);

            // Ensure that the share exists.
            if (share.Exists())
            {
                try
                {
                    // Get a reference to the root directory for the share.
                    CloudFileDirectory rootDir = share.GetRootDirectoryReference();
                    CloudFile cloudFile = rootDir.GetFileReference(fileName);
                    Stream stream = new MemoryStream(filebytes);
                    //Upload the file to Azure.
                    cloudFile.UploadFromStreamAsync(stream).Wait();
                    stream.Dispose();
                }
                catch (Exception e)
                {
                    throw new StorageAccountException("Error while attempting to upload", e);
                }
            }
            else
            {
                DirectoryNotFoundException e = new DirectoryNotFoundException(string.Format("The file share '{0}' does not exist.", fileShareName));
                throw new StorageAccountException("Error while attempting to upload", e);
            }



        }
        public void UploadToFileShare(string fileShareName, byte[] fileContents, string fileName = "")
        {
            //Create GUID to for filename if no name specified
            if (fileName.Length == 0)
            {
                fileName = Guid.NewGuid().ToString();
            }
            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);

            // Ensure that the share exists.
            if (share.Exists())
            {
                try
                {
                    // Get a reference to the root directory for the share.
                    CloudFileDirectory rootDir = share.GetRootDirectoryReference();
                    CloudFile cloudFile = rootDir.GetFileReference(fileName);
                    Stream stream = new MemoryStream(fileContents);
                    //Upload the file to Azure.
                    cloudFile.UploadFromStreamAsync(stream).Wait();
                    stream.Dispose();
                }
                catch (Exception e)
                {
                    throw new StorageAccountException("Error while attempting to upload", e);
                }

            }
            else
            {
                DirectoryNotFoundException e = new DirectoryNotFoundException(string.Format("The file share '{0}' does not exist.", fileShareName));
                throw new StorageAccountException("Error while attempting to upload", e);
            }



        }
        public string DownloadFileContentAsString(string fileShareName, string fileName = "")
        {

            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);

            // Ensure that the share exists.
            if (share.Exists())
            {
                try
                {
                    // Get a reference to the root directory for the share.
                    CloudFileDirectory rootDir = share.GetRootDirectoryReference();
                    CloudFile cloudFile = rootDir.GetFileReference(fileName);
                    return cloudFile.DownloadText();
                }
                catch (Exception e)
                {
                    throw new StorageAccountException("Error while attempting to get contents", e);
                }

            }
            else
            {
                DirectoryNotFoundException e = new DirectoryNotFoundException(string.Format("The file share '{0}' does not exist.", fileShareName));
                throw new StorageAccountException("Error while attempting to get content", e);
            }



        }
        public byte[] DownloadFileContentAsByteArray(string fileShareName, string fileName = "")
        {

            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);

            // Ensure that the share exists.
            if (share.Exists())
            {
                try
                {
                    // Get a reference to the root directory for the share.
                    CloudFileDirectory rootDir = share.GetRootDirectoryReference();
                    CloudFile cloudFile = rootDir.GetFileReference(fileName);
                    byte[] array = new byte[cloudFile.Properties.Length];
                    cloudFile.DownloadToByteArray(array, 0);
                    return array;
                }
                catch (Exception e)
                {
                    throw new StorageAccountException("Error while attempting to get contents", e);
                }

            }
            else
            {
                DirectoryNotFoundException e = new DirectoryNotFoundException(string.Format("The file share '{0}' does not exist.", fileShareName));
                throw new StorageAccountException("Error while attempting to get content", e);
            }



        }
        public void DownloadFileToLocalPath(string fileShareName, string localPath, FileMode mode, string fileName = "")
        {
            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.


            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);

            // Ensure that the share exists.
            if (share.Exists())
            {
                try
                {
                    // Get a reference to the root directory for the share.
                    CloudFileDirectory rootDir = share.GetRootDirectoryReference();
                    CloudFile cloudFile = rootDir.GetFileReference(fileName);
                    switch (mode)
                    {
                        case FileMode.Append:
                            cloudFile.DownloadToFile(localPath, FileMode.Append);
                            break;
                        case FileMode.Create:
                            cloudFile.DownloadToFile(localPath, FileMode.Create);
                            break;
                        case FileMode.CreateNew:
                            cloudFile.DownloadToFile(localPath, FileMode.CreateNew);
                            break;
                        case FileMode.Open:
                            cloudFile.DownloadToFile(localPath, FileMode.Open);
                            break;
                        case FileMode.OpenOrCreate:
                            cloudFile.DownloadToFile(localPath, FileMode.OpenOrCreate);
                            break;
                        case FileMode.Truncate:
                            cloudFile.DownloadToFile(localPath, FileMode.Truncate);
                            break;
                        default:
                            break;
                    }

                }
                catch (Exception e)
                {
                    throw new StorageAccountException("Error while attempting to get contents", e);
                }

            }
            else
            {
                DirectoryNotFoundException e = new DirectoryNotFoundException(string.Format("The file share '{0}' does not exist.", fileShareName));
                throw new StorageAccountException("Error while attempting to get content", e);
            }



        }
        public Stream DownloadFileContentAsStream(string fileShareName, string fileName = "")
        {

            CloudFileClient fileClient = new CloudFileClient(fileURI, creds);
            // Create a CloudFileClient object for credentialed access to Azure Files.
            Stream s = null;

            // Get a reference to the file share we created previously.
            CloudFileShare share = fileClient.GetShareReference(fileShareName);

            // Ensure that the share exists.
            if (share.Exists())
            {
                try
                {
                    // Get a reference to the root directory for the share.
                    CloudFileDirectory rootDir = share.GetRootDirectoryReference();
                    CloudFile cloudFile = rootDir.GetFileReference(fileName);
                    cloudFile.DownloadToStream(s);
                    return s;
                }
                catch (Exception e)
                {
                    throw new StorageAccountException("Error while attempting to get contents", e);
                }

            }
            else
            {
                DirectoryNotFoundException e = new DirectoryNotFoundException(string.Format("The file share '{0}' does not exist.", fileShareName));
                throw new StorageAccountException("Error while attempting to get content", e);
            }



        }
        // Queues
        public string[] GetQueueNames()
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);
            try
            {
                return queueClient.ListQueues().Select(x => x.Name).ToArray();
            }
            catch (Exception e)
            {
                throw new StorageAccountException("Unable to retrieve queue names.", e);
            }


        }
        public bool QueueExists(string queueName)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);
            try
            {
                CloudQueue q = queueClient.GetQueueReference(queueName);

                return q.Exists();
            }
            catch (Exception e)
            {
                throw new StorageAccountException("Error occured while checking if '" + queueName + "' exists.", e);
            }

        }
        public string PopQueueAsString(string queueName)
        {
            string returnData = string.Empty;

            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            try
            {
                CloudQueue queue = queueClient.GetQueueReference(queueName);
                if (queue.ApproximateMessageCount != null || queue.ApproximateMessageCount > 0)
                {
                    CloudQueueMessage message = queue.GetMessage();
                    returnData = message.AsString;
                }


                return returnData;
            }
            catch (Exception e)
            {
                throw new StorageAccountException("Unable to get message from '" + queueName + "'.", e);
            }

        }
        public byte[] PopQueueAsBytes(string queueName)
        {
            byte[] returnData = null;

            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            try
            {
                CloudQueue queue = queueClient.GetQueueReference(queueName);
                if (queue.ApproximateMessageCount != null || queue.ApproximateMessageCount > 0)
                {
                    CloudQueueMessage message = queue.GetMessage();
                    returnData = message.AsBytes;
                }


                return returnData;
            }
            catch (Exception e)
            {
                throw new StorageAccountException("Unable to get message from '" + queueName + "'.", e);
            }

        }
        public bool QueueHasMessages(string queueName)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);
            try
            {
                return queue.ApproximateMessageCount == null ? false : (queue.ApproximateMessageCount > 0 ? false : true);
            }
            catch (Exception e)
            {
                throw new StorageAccountException("Unnable to message count from '" + queueName + "'", e);
            }

        }
        public void PushQueue(string queueName, string dataToPush)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);
            CloudQueueMessage message = new CloudQueueMessage(dataToPush);
            try
            {
                queue.AddMessage(message);
            }
            catch (Exception e)
            {
                throw new StorageAccountException("Unable to push queue '" + queueName + "'", e);
            }



        }
        public void PushQueue(string queueName, byte[] dataToPush)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);
            CloudQueueMessage message = new CloudQueueMessage(dataToPush);
            queue.AddMessage(message);

        }
        public void PushQueue(string queueName, string messageId, string popReciept)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);
            CloudQueueMessage message = new CloudQueueMessage(messageId, popReciept);
            queue.AddMessage(message);

        }
        public void CreateQueue(string queueName)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);
            queue.CreateIfNotExists();
        }
        public void DeleteQueue(string queueName)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);

            queue.DeleteIfExists();
        }
        public void ClearQueue(string queueName)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);

            queue.Clear();
        }
        public string PeekAheadSingleMessage(string queueName)
        {
            CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

            CloudQueue queue = queueClient.GetQueueReference(queueName);

            return queue.PeekMessage().AsString;
        }
        public List<string> PeekAheadMultipleMessage(string queueName, int messagesAhead)
        {
            if (messagesAhead <= 32)
            {
                List<string> returnMessages = new List<string>();
                CloudQueueClient queueClient = new CloudQueueClient(queueURI, creds);

                CloudQueue queue = queueClient.GetQueueReference(queueName);


                List<CloudQueueMessage> messages = queue.PeekMessages(messagesAhead).ToList();
                foreach (CloudQueueMessage m in messages)
                {
                    returnMessages.Add(m.AsString);
                }
                return returnMessages;
            }
            else
            {
                throw new StorageAccountException("Only 32 Messages Maximum are allowed to peek ahead");
            }

        }
        //Tables
        public void AddOrReplaceItemOnTable(TableRow row, string tableName)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);
            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.InsertOrReplace(row.GetEntityToUpload());

            // Execute the insert operation.
            table.Execute(insertOperation);
        }
        public void AddItemsToTable(List<TableRow> rows, string tableName)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);
            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            foreach (TableRow t in rows)
            {
                batchOperation.Insert(t.GetEntityToUpload());
            }

            // Execute the insert operation.
            table.ExecuteBatch(batchOperation);
        }
        public List<TableRow> GetAllFromTable(string tableName)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);

            List<DynamicTableEntity> rows = table.ExecuteQuery(new TableQuery()).ToList();
            List<TableRow> returnSet = new List<TableRow>();

            foreach (DynamicTableEntity ent in rows)
            {
                TableRow temp = new TableRow(ent);

                returnSet.Add(temp);
            }

            return returnSet;

        }
        public List<TableRow> GetAllFromTablePartition(string tableName, string partition)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);

            List<DynamicTableEntity> rows = table.ExecuteQuery(new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partition))).ToList();
            List<TableRow> returnSet = new List<TableRow>();

            foreach (DynamicTableEntity ent in rows)
            {
                TableRow temp = new TableRow(ent);

                returnSet.Add(temp);
            }

            return returnSet;

        }
        public TableRow GetEntityFromTable(string tableName, string partitionKey, string rowKey)
        {
            TableRow returnDict;
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve(partitionKey, rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Print the phone number of the result.
            if (retrievedResult.Result != null)
            {
                DynamicTableEntity result = (DynamicTableEntity)retrievedResult.Result;
                returnDict = new TableRow(result);
            }
            else
            {
                throw new KeyNotFoundException(string.Format("No entry with row key '{0}' and parition key '{1}' exists in table '{2}'", rowKey, partitionKey, tableName));
            }

            return returnDict;
        }
        public void DeleteEntityFromTable(string tableName, string partitionKey, string rowKey)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);
            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<DynamicTableEntity>(partitionKey, rowKey);

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity.
            DynamicTableEntity deleteEntity = (DynamicTableEntity)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                table.Execute(deleteOperation);
            }
            else
            {
                throw new KeyNotFoundException(string.Format("No element with Partiton Key '{0}' and Row Key '{1}' exists in table '{2}'", partitionKey, rowKey, tableName));
            }
        }
        public List<TableRow> GetItemsWithSelectPropertiesFromTable(string tableName, string[] propertiesDesired)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);

            // Define the query, and select only the Email property.
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>().Select(propertiesDesired);
            EntityResolver<TableRow> resolver = (pk, rk, ts, props, etag) =>
            {
                TableRow temp = new TableRow(rk, pk);
                temp.UpdateValue("Timestamp", ts.ToString());
                temp.UpdateValue("ETag", etag);
                foreach (KeyValuePair<string, EntityProperty> pair in props)
                {
                    temp.AddColumnAndValue(pair.Key, pair.Value.StringValue);
                }
                // Potentially throw here if an unknown shape is detected 


                return temp;
            };
            return table.ExecuteQuery(projectionQuery, resolver, null, null).ToList();

        }
        public bool CheckIfTableExists(string tableName)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);
            return table.Exists();
        }
        public void CreateTableIfNotExists(string tableName)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);

            table.CreateIfNotExists();
        }
        public void DeleteTableIfExists(string tableName)
        {
            // Create the table client.
            CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

            // Create the CloudTable object that represents the "people" table.
            CloudTable table = tableClient.GetTableReference(tableName);
            // Delete the table it if exists.
            table.DeleteIfExists();
        }
        //public TableSegment GetQueryFromTableInSegments(string tableName, string continueToken)
        //{
        //    // Create the table client.
        //    CloudTableClient tableClient = new CloudTableClient(tableURI, tableCreds);

        //    // Create the CloudTable object that represents the "people" table.
        //    CloudTable table = tableClient.GetTableReference(tableName);

        //    // Initialize a default TableQuery to retrieve all the entities in the table.
        //    TableQuery<DynamicTableEntity> tableQuery = new TableQuery<DynamicTableEntity>();

        //    // Initialize the continuation token to null to start from the beginning of the table.
        //    TableContinuationToken continuationToken = null;
        //    if (!string.IsNullOrEmpty(continueToken))
        //    {
        //        continuationToken = new TableContinuationToken();
        //        XmlReader reader = XmlReader.Create(new StringReader(continueToken));
        //        continuationToken.ReadXml(reader);
        //    }


        //    // Retrieve a segment (up to 1,000 entities).
        //    AsyncBox holder = new AsyncBox();
        //    holder = QueryAsync(tableQuery, table, continuationToken).Result;
        //    TableSegment seg = new TableSegment();
        //    foreach (DynamicTableEntity entity in holder.tableQuerySegment.Results)
        //    {
        //        seg.AddRowToSegement(entity);
        //    }
        //    StringWriter sw = new StringWriter();
        //    XmlWriter writer = XmlWriter.Create(sw);
        //    continuationToken.WriteXml(writer);
        //    seg.SetContinuationToken(sw.ToString());

        //    return seg;
        //}
        private async System.Threading.Tasks.Task<AsyncBox> QueryAsync(TableQuery<DynamicTableEntity> tableQuery, CloudTable table, TableContinuationToken token)
        {
            AsyncBox temp = new AsyncBox();
            TableQuerySegment<DynamicTableEntity> tableQuerySegment = await table.ExecuteQuerySegmentedAsync(tableQuery, token);
            temp.token = token;
            temp.tableQuerySegment = tableQuerySegment;
            return temp;

        }
        //Blobs
        public bool CreateBlobContainerIfNotExists(string containerName)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            return cloudBlobContainer.CreateIfNotExists();

        }
        public bool DeleteBlobContainerIfExists(string containerName)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            return cloudBlobContainer.DeleteIfExists();

        }
        public void UploadAsBlob(string containerName, string localPathToFile)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            string fileName = localPathToFile.Split('/')[localPathToFile.Split('/').Length - 1].Split('.')[0];
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(fileName);
            cloudBlockBlob.UploadFromFile(localPathToFile);

        } //Check if you can pass in blob uri instead of name
        public void UploadAsBlob(string containerName, byte[] data, string blobName)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            cloudBlockBlob.UploadFromByteArray(data, 0, data.Length);

        }
        public void UploadAsBlob(string containerName, Stream dataStream, string blobName)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            cloudBlockBlob.UploadFromStream(dataStream);

        }
        public void UploadAsBlob(string containerName, string content, string blobName)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            cloudBlockBlob.UploadText(content);

        }
        public string DownloadBlobAsString(string containerName, string blobName)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            return cloudBlockBlob.DownloadText();

        }
        public void DownloadBlobToLocalPath(string containerName, string blobName, string localFilePathFull, FileMode mode)
        {
            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            switch (mode)
            {
                case FileMode.Append:
                    cloudBlockBlob.DownloadToFile(localFilePathFull, FileMode.Append);
                    break;
                case FileMode.Create:
                    cloudBlockBlob.DownloadToFile(localFilePathFull, FileMode.Create);
                    break;
                case FileMode.CreateNew:
                    cloudBlockBlob.DownloadToFile(localFilePathFull, FileMode.CreateNew);
                    break;
                case FileMode.Open:
                    cloudBlockBlob.DownloadToFile(localFilePathFull, FileMode.Open);
                    break;
                case FileMode.OpenOrCreate:
                    cloudBlockBlob.DownloadToFile(localFilePathFull, FileMode.OpenOrCreate);
                    break;
                case FileMode.Truncate:
                    cloudBlockBlob.DownloadToFile(localFilePathFull, FileMode.Truncate);
                    break;
                default:
                    break;
            }


        }
        public byte[] DownloadBlobAsByteArray(string containerName, string blobName)
        {

            // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);

            // Create a container called 'quickstartblobs' and append a GUID value to it to make the name unique. 
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            cloudBlockBlob.FetchAttributes();
            byte[] results = new byte[cloudBlockBlob.Properties.Length];
            cloudBlockBlob.DownloadToByteArray(results, 0);
            return results;

        }
        public List<string> ListBlobURIsInContainer(string containerName)
        {
            List<string> blobs = new List<string>();
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);
            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            // List the blobs in the container.
            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var results = cloudBlobContainer.ListBlobsSegmented(null, blobContinuationToken);
                // Get the value of the continuation token returned by the listing call.
                blobContinuationToken = results.ContinuationToken;
                foreach (IListBlobItem item in results.Results)
                {
                    blobs.Add(item.Uri.ToString());
                }
                blobContinuationToken = results.ContinuationToken;
            } while (blobContinuationToken != null); // Loop while the continuation token is not null. 
            return blobs;
        }
        public List<string> ListContainersURIs()
        {
            CloudBlobClient cloudBlobClient = new CloudBlobClient(blobURI, creds);
            List<string> containers = cloudBlobClient.ListContainers().Select(container => container.Name).ToList();
            return containers;
        }


    }
}

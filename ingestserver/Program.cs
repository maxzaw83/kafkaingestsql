using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using System.Text.Json;

// --- Configuration ---
// These variables are loaded from the Docker Compose environment file.
const string KafkaBootstrapServers = "kafka:9092";
const string KafkaTopicName = "IngestTopic";
const string SqlConnectionString = "Server=mssql;Database=KafkaDataDB;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True";

// --- Kafka Consumer Configuration ---
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = KafkaBootstrapServers,
    GroupId = "sql-ingest-group-1", // Unique ID for this consumer instance
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false // We commit manually after successful SQL insert
};

Console.WriteLine($"Kafka Ingest Service Initializing. Connecting to Kafka at {KafkaBootstrapServers}...");

// FIX APPLIED: Changed the key type from <Ignore, string> to <string, string>
// This resolves the compilation errors (CS1503 and CS0019).
using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
consumer.Subscribe(KafkaTopicName);

try
{
    while (true) // Continuous loop to consume messages
    {
        // Poll for a message, waiting up to 1 second
        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));

        if (consumeResult != null)
        {
            Console.WriteLine($"\n[INFO] Consumed Message: Topic='{consumeResult.Topic}' | Offset={consumeResult.Offset}");

            // Key is now correctly treated as a nullable string (string?)
            Console.WriteLine($"[DATA] Key: {consumeResult.Message.Key ?? "N/A"} | Value: {consumeResult.Message.Value}");

            // Attempt to insert into SQL
            if (TryIngestToSql(consumeResult.Topic, consumeResult.Message.Key, consumeResult.Message.Value, SqlConnectionString))
            {
                // Commit the offset only if SQL insertion was successful
                consumer.Commit(consumeResult);
                Console.WriteLine("[SUCCESS] Message processed and offset committed.");
            }
            else
            {
                // If SQL failed, we skip commit. The message will be retried on next poll/restart.
                Console.WriteLine("[FAILURE] Message not committed. Will retry later.");
            }
        }
    }
}
catch (OperationCanceledException)
{
    // Graceful shutdown
    Console.WriteLine("Application shutdown requested.");
}
finally
{
    consumer.Close();
}

// --- SQL Ingestion Method ---
static bool TryIngestToSql(string topic, string key, string value, string connectionString)
{
    try
    {
        // The connection server 'mssql' resolves to the mssql service container
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var sql = @"INSERT INTO KafkaIngestData (KafkaTopic, MessageKey, MessageValue) 
                    VALUES (@Topic, @Key, @Value)";

        using var command = new SqlCommand(sql, connection);
        // Using parameters prevents SQL Injection
        command.Parameters.AddWithValue("@Topic", topic);

        // Ensure that if 'key' is null, we pass DBNull.Value to SQL
        command.Parameters.AddWithValue("@Key", (object)key ?? DBNull.Value);
        command.Parameters.AddWithValue("@Value", value);

        command.ExecuteNonQuery();
        return true;
    }
    catch (SqlException ex)
    {
        Console.WriteLine($"[SQL ERROR] {ex.Message}");
        return false;
    }
}

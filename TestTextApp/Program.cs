

using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace TestTextApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "Data Source=DESKTOP-1RO9SM1;Initial catalog = master; Integrated Security=True";
            string dbName = "WordsDb";
            string tableName = "Words";
            string file1 = "text1.txt";
            string file2 = "text2.txt";
            string filePath = file1;
            bool result1 = AddFile(file1, connectionString, tableName, dbName);
            bool result2 = AddFile(file2, connectionString, tableName, dbName);
            Console.WriteLine($"{result1}, {result2}");
            Console.ReadKey();
        }
        static bool AddFile(string filePath, string connectionString, string tableName, string dbName)
        {
            bool result = EnsureDBExist(connectionString, dbName);
            if (!result)
            {
                ErrorHandler();
                return result;
            }
            else
            {
                result = EnsureTableExist(connectionString, tableName, dbName);
                if (!result)
                {
                    ErrorHandler();
                    return result;
                }
                else
                {
                    var words = WordsCounts(filePath);
                    result = FillDb(words, connectionString, tableName,dbName);
                    if (result)
                    {
                        Console.WriteLine("Слова успешно добавлены в базу данных");
                        return result;
                    }
                    else
                    {
                        ErrorHandler();
                        return result;
                    }
                }
            }
        }
        static void ErrorHandler() {
            Console.WriteLine("Ошибка при выполнении");
            Console.ReadKey();
        }
        static bool EnsureDBExist(string connectionString, string databaseName)
        {
            try
            {
                Console.WriteLine(connectionString);
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string checkDatabaseQuery = @$"
IF NOT EXISTS (
    SELECT name FROM sys.databases WHERE name = N'{databaseName}'
)
BEGIN 
    CREATE DATABASE [{databaseName}] 
END;";
                    using (SqlCommand command = new SqlCommand(checkDatabaseQuery, connection))
                    {
                        command.ExecuteNonQuery();
                        return true; 
                    }
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Ошибка при работе с базой данных: {ex.Message}");
                return false;
            }
        }
        static bool EnsureTableExist(string connectionString, string tableName, string dbName)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string checkTableExistQuery = $@"
IF NOT EXISTS (
    SELECT * FROM {dbName}.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = '{tableName}'
)
BEGIN
    EXEC('
        CREATE TABLE {dbName}.dbo.{tableName} (
            Id INT PRIMARY KEY IDENTITY(1,1),
            Word NVARCHAR(20) NOT NULL,
            Frequency INT NOT NULL
        )
    ');
END;";
                    using (SqlCommand command = new SqlCommand(checkTableExistQuery, connection))
                    {
                        command.ExecuteNonQuery();
                        return true;
                    }
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Ошибка в работе с базой данных: {ex.Message}");
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка работы программы: {ex.Message}");
                return false;
            }
        }
        static bool FillDb(IEnumerable<KeyValuePair<string, int>> words, string connectionString, string tableName,string dbName)
        {
            try
            {
                var wordsInTable = new Dictionary<string, int>();
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    // Use separate method to handle the data reader
                    wordsInTable = ReadExistingWords(connection, tableName, wordsInTable, dbName);

                    foreach (var word in words)
                    {
                        if (wordsInTable.ContainsKey(word.Key))
                        {
                            string updateQuery = $@"
UPDATE {dbName}.dbo.{tableName} SET Frequency = @NewFrequency WHERE Word = @Word";
                            using (SqlCommand updateCmd = new SqlCommand(updateQuery, connection))
                            {
                                updateCmd.Parameters.AddWithValue("@NewFrequency", wordsInTable[word.Key] + word.Value);
                                updateCmd.Parameters.AddWithValue("@Word", word.Key);
                                updateCmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            string insertQuery = $@"
INSERT INTO {dbName}.dbo.{tableName} (Word, Frequency) VALUES (@Word, @Frequency)";
                            using (SqlCommand insertCmd = new SqlCommand(insertQuery, connection))
                            {
                                insertCmd.Parameters.AddWithValue("@Word", word.Key);
                                insertCmd.Parameters.AddWithValue("@Frequency", word.Value);
                                insertCmd.ExecuteNonQuery();
                            }
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        static Dictionary<string, int> ReadExistingWords(SqlConnection connection, string tableName, Dictionary<string, int> wordsInTable,string dbName)
        {
            string selectAllQuery = $@"
SELECT Frequency, Word FROM {dbName}.dbo.{tableName}";
            using (SqlCommand command = new SqlCommand(selectAllQuery, connection))
            {
                using (var dataReader = command.ExecuteReader())
                {
                    while (dataReader.Read())
                    {
                        int frequency = dataReader.GetInt32(0);
                        string existedWord = dataReader.GetString(1);

                        wordsInTable[existedWord] = frequency;
                    }
                }
            }
            return wordsInTable;
        }
        static Dictionary<string, int> WordsCounts(string path)
        {
            var wordPairs = new Dictionary<string, int>();

            using (StreamReader reader = new StreamReader(path, Encoding.UTF8))
            {
                string? line;
                while ((line = reader.ReadLine()) != null) 
                {
                    var words = Regex.Matches(line.ToLower(), @"\b\w{3,20}\b", RegexOptions.CultureInvariant);

                    foreach (Match match in words)
                    {
                        string word = match.Value.ToLower();

                        if (wordPairs.ContainsKey(word))
                        {
                            wordPairs[word]++;
                        }
                        else
                        {
                            wordPairs[word] = 1;
                        }
                    }
                }
            }
            var filteredWords = wordPairs.Where(w => w.Value >= 4).ToDictionary(w => w.Key, w => w.Value);

            return filteredWords;
        }
    }
}

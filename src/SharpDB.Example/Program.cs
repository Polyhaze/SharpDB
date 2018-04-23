using SharpDB.Driver;
using System;

namespace SharpDB.Example
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            using (SharpDBClient client = new SharpDBClient("tcp://127.0.0.1:5999"))
            {
                using (SharpDBConnection connection = client.GetConnection())
                {
                    Account newAccount = new Account();
                    newAccount.Name = "Hello";
                    newAccount.Id = 1;
                    connection.Update(newAccount);
                    Account storedAccount = connection.Get<Account>(1);
                    connection.DeleteDocument(newAccount);
                    Console.WriteLine("Hello" == storedAccount.Name);
                }
            }
        }

        public class Account
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }
    }
}
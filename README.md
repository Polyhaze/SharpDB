SharpDB
=====

SharpDB is C# based High Performance Key Value NoSQL Database with ACID Transaction.
SharpDB is using [Multiversion concurrency control](http://en.wikipedia.org/wiki/Multiversion_concurrency_control) to achieve transactions.
Because of using MVCC SharpDB is very fast in doing transaction (the downside is that SharpDB using a lof of space).

For network protocl SharpDB is using [NetMQ](https://github.com/zeromq/netmq) which is C# port of ZeroMQ, which means SharpDB is using very fast on the network as well.

## Installation

Packages of SharpDB is still not available, so download the code code and compile.

After compiling run the following command to run the server:
 
	SharpDB.Server.exe run -name:test -port:5555

You can change the name and port fields as you like. File called name.sdb will be created as the database file.

You can also run the database as a service with the following commands (must run as administrator)
  
	SharpDB.Server.exe install -name:test -port:5555
	SharpDB.Server.exe start
  
## Using

To use SharpDB from your application include the SharpDB.Driver assembly. 
Following is small example of using the SharpDB driver:

		[Test]
		public void UpdateGet()
		{
			using (SharpDBClient client = new SharpDBClient("tcp://127.0.0.1:5555"))
			{
				using (SharpDBConnection connection = client.GetConnection())
				{
					Account newAccount = new Account();
					newAccount.Name = "Hello";
					newAccount.Id = 1;

					connection.Update(newAccount);

					Account storedAccount = connection.Get<Account>(1);

					Assert.AreEqual("Hello", storedAccount.Name);
				}
			}
		}
		
and the account class:
	public class Account
	{
		public int Id { get; set; }

		public string Name { get; set; }
	}

## Contributing

I really appricate help with the project, just click the fork button, pick an issue from the issues (or add your own issue) and create a pull request.
If you will send some good pull request you will commit permission to the repository.

## Who owns SharpDB?

SharpDB is owned by all its authors and contributors. 
This is an open source project licensed under the LGPLv3.

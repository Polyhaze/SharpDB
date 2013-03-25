namespace SomDB.Engine.Cache
{
	public interface ICacheProvider
	{
		string Name { get; set; }
		void Set(long fileLocation, byte[] blob);
		byte[] Get(long fileLocation);
	}
}
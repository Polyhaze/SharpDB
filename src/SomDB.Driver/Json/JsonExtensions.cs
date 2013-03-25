using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;

namespace SomDB.Driver.Json
{
	public static class JsonExtensions
	{
		public static JObject GetJObject(this SomDBConnection connection, object documentId)
		{
			byte[] documentIdBytes = connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = connection.Serializer as BsonSerializer;

			byte[] blob = connection.GetInternal(documentIdBytes);

			return serializer.DeserializeToJObject(blob);
		}

		public static void UpdateJObject(this SomDBConnection connection, JObject jobject)
		{
			JValue idToken = (JValue) jobject["Id"];

			object documentId = idToken.Value;

			byte[] documentIdBytes = connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = connection.Serializer as BsonSerializer;

			byte[] blob = serializer.SerializeFronJObject(jobject);

			connection.UpdateInternal(documentIdBytes, blob);
		}

		public static void DeleteJObject(this SomDBConnection connection, JObject jobject)
		{
			JValue idToken = (JValue)jobject["Id"];

			DeleteJObject(connection, idToken);
		}

		public static void DeleteJObject(this SomDBConnection connection, JValue idToken)
		{			
			object documentId = idToken.Value;

			byte[] documentIdBytes = connection.Serializer.SerializeDocumentId(documentId);
			
			connection.DeleteInternal(documentIdBytes);
		}

		public static JObject GetJObject(this SomDBTransaction transaction, object documentId)
		{
			byte[] documentIdBytes = transaction.Connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = transaction.Connection.Serializer as BsonSerializer;

			byte[] blob = transaction.Connection.GetInternal(documentIdBytes, transaction);

			return serializer.DeserializeToJObject(blob);
		}

		public static void UpdateJObject(this SomDBTransaction transaction, JObject jobject)
		{
			JValue idToken = (JValue)jobject["Id"];

			object documentId = idToken.Value;

			byte[] documentIdBytes = transaction.Connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = transaction.Connection.Serializer as BsonSerializer;

			byte[] blob = serializer.SerializeFronJObject(jobject);

			transaction.Connection.UpdateInternal(documentIdBytes, blob, transaction);
		}

		public static void DeleteJObject(this SomDBTransaction transaction, JObject jobject)
		{
			JValue idToken = (JValue)jobject["Id"];

			DeleteJObject(transaction, idToken);
		}

		public static void DeleteJObject(this SomDBTransaction transaction, JValue idToken)
		{
			object documentId = idToken.Value;

			byte[] documentIdBytes = transaction.Connection.Serializer.SerializeDocumentId(documentId);

			transaction.Connection.DeleteInternal(documentIdBytes, transaction);
		}
	}
}

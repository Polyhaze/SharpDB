using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;

namespace SharpDB.Driver.Json
{
	public static class JsonExtensions
	{
		public static JObject GetJObject(this SharpDBConnection connection, object documentId)
		{
			byte[] documentIdBytes = connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = connection.Serializer as BsonSerializer;

			byte[] blob = connection.GetInternal(documentIdBytes);

			return serializer.DeserializeToJObject(blob);
		}

		public static void UpdateJObject(this SharpDBConnection connection, JObject jobject)
		{
			JValue idToken = (JValue) jobject["Id"];

			object documentId = idToken.Value;

			byte[] documentIdBytes = connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = connection.Serializer as BsonSerializer;

			byte[] blob = serializer.SerializeFronJObject(jobject);

			connection.UpdateInternal(documentIdBytes, blob);
		}

		public static void DeleteJObject(this SharpDBConnection connection, JObject jobject)
		{
			JValue idToken = (JValue)jobject["Id"];

			DeleteJObject(connection, idToken);
		}

		public static void DeleteJObject(this SharpDBConnection connection, JValue idToken)
		{			
			object documentId = idToken.Value;

			byte[] documentIdBytes = connection.Serializer.SerializeDocumentId(documentId);
			
			connection.DeleteInternal(documentIdBytes);
		}

		public static JObject GetJObject(this SharpDBTransaction transaction, object documentId)
		{
			byte[] documentIdBytes = transaction.Connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = transaction.Connection.Serializer as BsonSerializer;

			byte[] blob = transaction.Connection.GetInternal(documentIdBytes, transaction);

			return serializer.DeserializeToJObject(blob);
		}

		public static void UpdateJObject(this SharpDBTransaction transaction, JObject jobject)
		{
			JValue idToken = (JValue)jobject["Id"];

			object documentId = idToken.Value;

			byte[] documentIdBytes = transaction.Connection.Serializer.SerializeDocumentId(documentId);

			BsonSerializer serializer = transaction.Connection.Serializer as BsonSerializer;

			byte[] blob = serializer.SerializeFronJObject(jobject);

			transaction.Connection.UpdateInternal(documentIdBytes, blob, transaction);
		}

		public static void DeleteJObject(this SharpDBTransaction transaction, JObject jobject)
		{
			JValue idToken = (JValue)jobject["Id"];

			DeleteJObject(transaction, idToken);
		}

		public static void DeleteJObject(this SharpDBTransaction transaction, JValue idToken)
		{
			object documentId = idToken.Value;

			byte[] documentIdBytes = transaction.Connection.Serializer.SerializeDocumentId(documentId);

			transaction.Connection.DeleteInternal(documentIdBytes, transaction);
		}
	}
}

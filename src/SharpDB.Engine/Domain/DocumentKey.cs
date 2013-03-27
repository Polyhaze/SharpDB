using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SharpDB.Engine.Domain
{
	public class DocumentId
	{
		private int m_hash;

		public DocumentId(string id)
			: this(Encoding.Unicode.GetBytes(id))
		{

		}

		public DocumentId(int id)
			: this(BitConverter.GetBytes(id))
		{

		}

		public DocumentId(byte[] value)
		{
			Bytes = value;
		}

		public byte[] Bytes { get; private set; }

		public int Length
		{
			get { return Bytes.Length; }
		}

		public string GetBytesReprestnation()
		{
			StringBuilder bytesRepresentation = new StringBuilder();

			for (int i = 0; i < Bytes.Length - 1; i++)
			{
				bytesRepresentation.AppendFormat("{0},", (int)Bytes[i]);
			}

			bytesRepresentation.AppendFormat("{0}", (int) Bytes.Last());

			return bytesRepresentation.ToString();
		}

		protected bool Equals(DocumentId other)
		{
			if (other.Bytes.Length != Bytes.Length)
			{
				return false;
			}

			return Bytes.SequenceEqual(other.Bytes);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;

			if (obj.GetType() != this.GetType()) return false;
			return Equals((DocumentId)obj);
		}

		public override int GetHashCode()
		{
			if (m_hash == 0)
			{
				foreach (byte b in Bytes)
				{
					m_hash = 31 * m_hash + b;
				}
			}
			return m_hash;
		}
	}
}

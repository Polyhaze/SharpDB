using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Logging;
using Common.Logging.Simple;
using NetMQ;
using SharpDB.Engine;
using SharpDB.Engine.Cache;
using SharpDB.Engine.IO;
using Topshelf;

namespace SharpDB.Server
{
#if NET40

    public class ServerService
#else

    public class ServerService : ServiceControl
#endif
    {
        private readonly string m_name;
        private readonly int m_port;
        private KeyValueDatabase m_db;
        private Network.Server m_server;
        private ILog m_log;
        private Task m_task;

        public ServerService(string name, int port)
        {
            m_name = name;
            m_port = port;
        }

#if NET40

        public bool Start()
#else

        public bool Start(HostControl hostControl)
#endif
        {
            m_log = LogManager.GetLogger(this.GetType());

            m_log.InfoFormat("Starting SharpDB...");
            m_log.InfoFormat("Database Name: {0}", m_name);

            m_db = new KeyValueDatabase(filename => new DatabaseFileReader(filename), filename => new DatabaseFileWriter(filename),
                                                                     filename => new MemoryCacheProvider(filename));
            m_db.FileName = m_name + ".sdb";
            m_db.Start();

            m_server = new Network.Server(m_db, string.Format("tcp://*:{0}", m_port));

            m_task = Task.Factory.StartNew(m_server.Start);
            return true;
        }

#if NET40

        public bool Stop()
#else

        public bool Stop(HostControl hostControl)
#endif
        {
            m_log.InfoFormat("Stopping SharpDB...");

            m_server.Stop();
            m_task.Wait();

            m_db.Stop();

            m_server = null;
            m_db = null;
            return true;
        }
    }
}
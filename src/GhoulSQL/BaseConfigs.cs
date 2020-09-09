using Ghoul.Configuration;
using System.Collections.Generic;

namespace GhoulSQL
{

    public class Configs
    {
        private static DBConfig m_db_config = null;
        public static DBConfig DB
        {
            get
            {
                if (m_db_config == null)
                {
                    lock (typeof(DBConfig))
                    {
                        if (m_db_config == null)
                            m_db_config = JsonConfiguration.AppSettings.Get<DBConfig>("DB");
                    }
                }
                return m_db_config;
            }
            set
            {
                m_db_config = value;
            }
        }
    }


    /// <summary>
    /// DB Config
    /// </summary>
    public class DBConfig
    {
        public bool Debug { get; set; }
        /// <summary>
        /// The master connection.
        /// </summary>
        /// <value>
        /// The master connection.
        /// </value>
        public string MasterConnection { get; set; }
        /// <summary>
        /// The slave connections.
        /// </summary>
        /// <value>
        /// The slave connections.
        /// </value>
        public IEnumerable<string> SlaveConnections { get; set; }
    }
}

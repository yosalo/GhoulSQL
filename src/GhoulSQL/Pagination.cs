using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace GhoulSQL
{
    /// <summary>
    /// Pagination
    /// </summary>
    [DataContract]
    [Serializable]
    public class Pagination<T>
    {

        public Pagination(IEnumerable<T> sourceData, int pageIndex, int pageSize, int totalCount, Hashtable totals = null)
        {
            this.Data = sourceData;
            this.PageIndex = pageIndex;
            this.PageSize = pageSize;
            this.TotalCount = totalCount;
            this.Totals = totals;
        }

        /// <summary>
        /// PageSize
        /// </summary>
        [DataMember]
        public int PageSize { get; set; }

        /// <summary>
        /// PageIndex
        /// </summary>
        [DataMember]
        public int PageIndex { get; set; }

        /// <summary>
        /// PageCount
        /// </summary>
        [DataMember]
        public int PageCount { get; set; }

        /// <summary>
        /// TotalCount
        /// </summary>
        [DataMember]
        public int TotalCount { get; set; }

        /// <summary>
        /// Totals
        /// </summary>
        /// <value>
        /// The totals.
        /// </value>
        [DataMember]
        public Hashtable Totals { get; set; }


        /// <summary>
        /// Data
        /// </summary>
        [DataMember]
        public IEnumerable<T> Data { get; set; }
    }
}

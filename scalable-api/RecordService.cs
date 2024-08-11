using Common.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using MySql.Data.MySqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Configuration;

namespace ProcessingService.Database
{

	public class RecordService : IRecordService
	{
		private readonly ILog _Log = LogManager.GetLogger<RecordService>();
		private readonly ISqlMapper _DB;
		private readonly ITransactionManager _Transaction;
		private readonly ITenant _Tenant;
		private readonly IRecordsIngressService _Ingress;

		public RecordService(
			IDbRegistry dbs,
			ITransactionManager transaction,
			IRecordsIngressService ingress)
		{
			_DB = dbs.GetMapper(DatabaseServiceModule.Connection);
			_Tenant = tenant;
			_Ingress = ingress;
		}


		public void Submit(IEnumerable<Record> records)
		{
			try {

				_Ingress.Submit(records);

			} catch (Exception e)
			{
				_Log.Error("Submit records Error: " + e);
			}

		}

		public void ProcessIngress(string consumerName)
		{
			foreach (var record in _Ingress.Yield(consumerName))
			{
				ProcessRecord(record);
				_Ingress.Acknowledge(consumerName, record.IngressId);
				_Ingress.Delete(consumerName, record.IngressId);
			}
		}
	}
}
